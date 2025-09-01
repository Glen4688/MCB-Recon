
import os
import io
import pandas as pd
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.client_credential import ClientCredential

# --- SharePoint Configuration ---
# These credentials should be set as Environment Variables in your Render dashboard.
SHAREPOINT_URL = os.environ.get("SHAREPOINT_URL") # e.g., "https://yourtenant.sharepoint.com/sites/yoursite"
CLIENT_ID = os.environ.get("SHAREPOINT_CLIENT_ID")
CLIENT_SECRET = os.environ.get("SHAREPOINT_CLIENT_SECRET")

# The SharePoint folder where the output report will be saved.
OUTPUT_FOLDER = 'Shared Documents/Reconciled Reports'

def get_sharepoint_context():
    """Creates and returns a SharePoint client context."""
    if not all([SHAREPOINT_URL, CLIENT_ID, CLIENT_SECRET]):
        raise ValueError("SharePoint environment variables are not set.")
    
    creds = ClientCredential(CLIENT_ID, CLIENT_SECRET)
    ctx = ClientContext(SHAREPOINT_URL).with_credentials(creds)
    return ctx

def get_file_from_sharepoint(file_url):
    """
    Downloads a file from a SharePoint URL and returns it as a pandas DataFrame.
    """
    ctx = get_sharepoint_context()
    
    # Use an in-memory binary stream to hold the file contents
    file_content = io.BytesIO()
    
    # Get the file from SharePoint and download its content
    file = ctx.web.get_file_by_server_relative_url(file_url).download(file_content).execute_query()
    
    # Reset the stream position to the beginning
    file_content.seek(0)
    
    # Read the Excel data from the in-memory stream
    df = pd.read_excel(file_content)
    print(f"Successfully downloaded and read file from: {file_url}")
    return df

def upload_file_to_sharepoint(df, file_name):
    """
    Saves a DataFrame to an in-memory Excel file and uploads it to SharePoint.
    Returns the URL of the uploaded file.
    """
    ctx = get_sharepoint_context()
    target_folder = ctx.web.get_folder_by_server_relative_url(OUTPUT_FOLDER)
    
    # Use an in-memory binary stream to save the Excel file
    output_buffer = io.BytesIO()
    df.to_excel(output_buffer, index=False)
    output_buffer.seek(0) # Rewind the buffer
    
    # Upload the file
    target_file = target_folder.upload_file(file_name, output_buffer).execute_query()
    
    print(f"Successfully uploaded file: {target_file.serverRelativeUrl}")
    
    # Return the full URL to the uploaded file
    return f"{SHAREPOINT_URL}{target_file.serverRelativeUrl}"
