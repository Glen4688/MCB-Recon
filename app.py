import os
import pandas as pd
from flask import Flask, request, jsonify
from recon_logic import run_reconciliation
import sharepoint_client

app = Flask(__name__)

@app.route("/reconcile", methods=['POST'])
def reconcile_endpoint():
    """
    Endpoint to run the reconciliation process.
    Expects a JSON payload with 'invoice_url' and 'po_url'.
    These URLs must be server-relative, e.g., '/sites/yoursite/Shared Documents/yourfile.xlsx'
    """
    try:
        # 1. Get SharePoint relative URLs from the request
        data = request.get_json()
        if not data or 'invoice_url' not in data or 'po_url' not in data:
            return jsonify({"error": "Missing 'invoice_url' or 'po_url' in request body"}), 400

        invoice_url = data['invoice_url']
        po_url = data['po_url']

        # 2. Download files from SharePoint
        print(f"Downloading files from SharePoint...")
        df_invoice = sharepoint_client.get_file_from_sharepoint(invoice_url)
        df_po = sharepoint_client.get_file_from_sharepoint(po_url)
        print("Files downloaded successfully.")

        # 3. Run the core reconciliation logic
        print("Running reconciliation logic...")
        final_df = run_reconciliation(df_invoice, df_po)
        print("Reconciliation logic complete.")

        # 4. Upload the resulting Excel file to SharePoint
        print("Uploading reconciled report to SharePoint...")
        output_url = sharepoint_client.upload_file_to_sharepoint(final_df, "MCB_Reconciled_Report.xlsx")
        print(f"Report uploaded successfully to {output_url}")

        # 5. Return the URL of the new report
        return jsonify({
            "message": "Reconciliation complete.",
            "output_url": output_url
        })

    except Exception as e:
        # Log the exception for debugging on Render
        print(f"An error occurred: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    # Gunicorn will be used in production, but this is for local testing.
    # Render sets the PORT environment variable.
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)