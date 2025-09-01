
import pandas as pd
import re
import numpy as np

def run_reconciliation(df_invoice, df_po):
    """
    Reconciles invoice metadata with PO report data using a multi-level, multi-pass matching strategy.
    Accepts two DataFrames and returns a final, reconciled DataFrame.
    """
    # --- Preprocessing ---
    df_invoice['invoice_line_id'] = df_invoice.index
    df_po['po_line_id'] = df_po.index

    def extract_customer_info(description):
        if pd.isna(description): return None, None
        parts = str(description).split('|')
        return (parts[0].strip() if len(parts) > 0 else None,
                parts[1].strip() if len(parts) > 1 else None)

    df_po[['Extracted_CustomerID', 'Extracted_CustomerName']] = df_po['Item Description'].apply(
        lambda x: pd.Series(extract_customer_info(x)))

    df_invoice_reconciled = df_invoice.copy()
    df_invoice_reconciled.rename(
        columns={'CustomerID': 'Invoice_CustomerID', 'Customer Name': 'Invoice_CustomerName', 'Unit Price': 'Unit_Price_Invoice'},
        inplace=True)
    
    # --- Triage Invoices based on CustomerID validity ---
    def is_valid_customer_id(cid):
        if pd.isna(cid): return False
        return bool(re.match(r'^[a-zA-Z0-9]{5}$', str(cid)))

    df_invoice_reconciled['is_valid_id'] = df_invoice_reconciled['Invoice_CustomerID'].apply(is_valid_customer_id)
    
    invoices_with_valid_id = df_invoice_reconciled[df_invoice_reconciled['is_valid_id']].copy()
    invoices_without_valid_id = df_invoice_reconciled[~df_invoice_reconciled['is_valid_id']].copy()

    # --- Pass 1: Direct ID Match (on invoices with a valid ID) ---
    id_merged = pd.merge(invoices_with_valid_id, df_po, left_on='Invoice_CustomerID', right_on='Extracted_CustomerID', how='left')
    
    id_match_success = id_merged.dropna(subset=['po_line_id']).copy()
    id_match_success['link_method'] = 'ID'
    
    id_match_fail_ids = id_merged[id_merged['po_line_id'].isna()]['invoice_line_id']
    invoices_for_pass3 = invoices_with_valid_id[invoices_with_valid_id['invoice_line_id'].isin(id_match_fail_ids)].copy()

    # --- Name Matching Helper Functions (ENHANCED) ---
    def clean_name_for_token_matching(name, is_invoice_name=False):
        if pd.isna(name): return ""
        name_str = str(name).lower()
        if is_invoice_name:
            name_str = name_str.split('-')[0]
        
        titles = ['mr', 'mrs', 'ms', 'miss', 'dr']
        for title in titles:
            name_str = re.sub(r'\b' + title + r'\.?( |$)', ' ', name_str)

        name_str = re.sub(r'[^a-z0-9\s]', '', name_str)
        return re.sub(r'\s+', ' ', name_str).strip()

    def check_name_match(row):
        invoice_name_tokens = set(str(row['clean_name_x']).split())
        po_name_tokens = set(str(row['clean_name_y']).split())
        if not po_name_tokens: return False
        return po_name_tokens.issubset(invoice_name_tokens)

    df_po['clean_name'] = df_po['Extracted_CustomerName'].apply(lambda x: clean_name_for_token_matching(x))

    # --- Pass 2: Name Match (for invoices with no/invalid ID) ---
    name_match_pass2_success = pd.DataFrame()
    unmatched_pass2 = invoices_without_valid_id.copy()

    if not invoices_without_valid_id.empty:
        invoices_without_valid_id.loc[:, 'clean_name'] = invoices_without_valid_id['Invoice_CustomerName'].apply(lambda x: clean_name_for_token_matching(x, is_invoice_name=True))
        cross_df = pd.merge(invoices_without_valid_id, df_po.dropna(subset=['clean_name']), how='cross')
        
        if not cross_df.empty:
            cross_df['name_match'] = cross_df.apply(check_name_match, axis=1)
            name_merged = cross_df[cross_df['name_match']].copy()
            name_merged['link_method'] = 'Name'
            name_match_pass2_success = name_merged.drop_duplicates(subset=['invoice_line_id'], keep='first')
        
        invoices_linked_pass2 = name_match_pass2_success['invoice_line_id'].unique()
        unmatched_pass2 = invoices_without_valid_id[~invoices_without_valid_id['invoice_line_id'].isin(invoices_linked_pass2)]

    # --- Pass 3: Discrepancy Check (Name Match for ID match failures) ---
    name_match_pass3_success = pd.DataFrame()
    unmatched_pass3 = invoices_for_pass3.copy()

    if not invoices_for_pass3.empty:
        invoices_for_pass3.loc[:, 'clean_name'] = invoices_for_pass3['Invoice_CustomerName'].apply(lambda x: clean_name_for_token_matching(x, is_invoice_name=True))
        cross_df = pd.merge(invoices_for_pass3, df_po.dropna(subset=['clean_name']), how='cross')

        if not cross_df.empty:
            cross_df['name_match'] = cross_df.apply(check_name_match, axis=1)
            name_merged = cross_df[cross_df['name_match']].copy()
            
            name_merged['link_method'] = 'ID_Mismatch_Name_Match'
            name_merged['Reconciliation_Notes'] = 'Inv CID ' + name_merged['Invoice_CustomerID'] + ' not in POs; name matched to PO CID ' + name_merged['Extracted_CustomerID'].astype(str)
            name_match_pass3_success = name_merged.drop_duplicates(subset=['invoice_line_id'], keep='first')

        invoices_linked_pass3 = name_match_pass3_success['invoice_line_id'].unique()
        unmatched_pass3 = invoices_for_pass3[~invoices_for_pass3['invoice_line_id'].isin(invoices_linked_pass3)]

    # --- Combine all potential links from all passes ---
    merged_df = pd.concat([id_match_success, name_match_pass2_success, name_match_pass3_success], ignore_index=True)

    # --- Processing Level 1: Find and claim perfect amount matches ---
    merged_df['Unit_Price_Invoice'] = pd.to_numeric(merged_df['Unit_Price_Invoice'], errors='coerce')
    merged_df['Ordered Amount'] = pd.to_numeric(merged_df['Ordered Amount'], errors='coerce')
    
    merged_df['is_perfect_match'] = np.isclose(merged_df['Unit_Price_Invoice'], merged_df['Ordered Amount'])
    perfect_candidates = merged_df[merged_df['is_perfect_match'] == True].copy()
    
    perfect_matches = perfect_candidates.drop_duplicates(subset=['invoice_line_id'], keep='first')
    perfect_matches = perfect_matches.drop_duplicates(subset=['po_line_id'], keep='first')
    perfect_matches['Match_Method'] = perfect_matches['link_method'] + '_Amount_Match'

    # --- Processing Level 2: Proportional allocation for remaining items ---
    claimed_invoice_ids = perfect_matches['invoice_line_id']
    claimed_po_ids = perfect_matches['po_line_id'].dropna()

    remaining_links = merged_df[(~merged_df['invoice_line_id'].isin(claimed_invoice_ids)) & (~merged_df['po_line_id'].isin(claimed_po_ids))]

    allocated_df = pd.DataFrame()
    if not remaining_links.empty:
        original_invoice_cols = df_invoice_reconciled.columns.tolist()
        po_cols_to_aggregate = [col for col in df_po.columns if col in remaining_links.columns]
        agg_funcs = {col: 'sum' if remaining_links[col].dtype in ['int64', 'float64'] else lambda x: ', '.join(x.dropna().astype(str).unique()) for col in po_cols_to_aggregate}
        
        aggregated_po_data = remaining_links.groupby('invoice_line_id')[po_cols_to_aggregate].agg(agg_funcs).reset_index()
        
        invoices_for_allocation = df_invoice_reconciled[df_invoice_reconciled['invoice_line_id'].isin(remaining_links['invoice_line_id'].unique())]
        allocated_df = pd.merge(invoices_for_allocation, aggregated_po_data, on='invoice_line_id', how='left')
        allocated_df['Match_Method'] = 'Customer_Allocated'

        for _, group in allocated_df.groupby('Invoice_CustomerID'):
            total_invoice = group['Unit_Price_Invoice'].sum()
            total_po = group['Ordered Amount'].sum()
            if total_invoice > 0 and total_po > 0:
                for idx, row in group.iterrows():
                    allocated_df.loc[idx, 'Ordered Amount'] = (row['Unit_Price_Invoice'] / total_invoice) * total_po

    # --- Final Combination ---
    unmatched_df = pd.concat([unmatched_pass2, unmatched_pass3], ignore_index=True)
    unmatched_df['Match_Method'] = 'Unmatched'

    final_df = pd.concat([perfect_matches, allocated_df, unmatched_df], ignore_index=True)
    final_df['Ordered Amount'] = final_df['Ordered Amount'].fillna(0)
    final_df['Amount_Difference'] = final_df['Unit_Price_Invoice'] - final_df['Ordered Amount']
    
    # Clean up helper columns
    final_df.drop(columns=['invoice_line_id', 'po_line_id', 'Extracted_CustomerID', 
                           'Extracted_CustomerName', 'is_perfect_match', 'link_method', 
                           'clean_name', 'clean_name_x', 'clean_name_y', 'name_match',
                           'is_valid_id'], errors='ignore', inplace=True)
    
    return final_df
