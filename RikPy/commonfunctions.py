import os
import requests
import datetime
import inspect
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import uuid
from urllib.parse import urlparse
from datetime import date
from dotenv import load_dotenv
from .customresponse import CustomResponse

def rfplogger(log_message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    calling_module = inspect.getframeinfo(inspect.currentframe().f_back).function
    log_entry = f"{timestamp} - {calling_module}: {log_message}\n"
    with open("logfile.txt", "a", encoding='utf-8') as log_file:
        log_file.write(log_entry)

def extract_file_extension(file_url):
    # Parse the URL to handle both formats
    parsed_url = urlparse(file_url)

    # Split the path component to get the filename
    filename = os.path.basename(parsed_url.path)

    # Split the filename to get the file extension
    _, file_extension = os.path.splitext(filename)

    # Clean up any query parameters that might be part of the extension
    file_extension = file_extension.split('?')[0]

    return file_extension

def download_file_local(url):
    local_filename = url.split('/')[-1]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192): 
                f.write(chunk)
    return local_filename

def download_file_local_with_query_parameters(file_url, generate_random_filename=False):
    response = requests.get(file_url)
    response.raise_for_status()  # Raise an error for bad responses

    # Extract the path from the URL without query parameters
    parsed_url = urlparse(file_url)
    original_filename = os.path.basename(parsed_url.path)
    original_extension = os.path.splitext(original_filename)[1]  # Includes the dot (e.g., '.png')

    # Generate random filename if requested, otherwise use the original filename
    if generate_random_filename:
        file_name = f"{uuid.uuid4()}{original_extension}"  # Keep original extension
    else:
        file_name = original_filename

    # file_name = os.path.basename(parsed_url.path)
    with open(file_name, 'wb') as file:
        file.write(response.content)
        
    return file_name

def delete_local_file(file_name):
    try:
        # Check if file exists
        if os.path.exists(file_name):
            # Delete the file
            os.remove(file_name)
            return f"File '{file_name}' has been deleted."
        else:
            return f"File '{file_name}' does not exist."

    except Exception as e:
        return f"An error occurred while deleting the file: {e}"

def download_image(image_url, local_file_path):
    response = requests.get(image_url)
    if response.status_code == 200:
        with open(local_file_path, 'wb') as file:
            file.write(response.content)
    else:
        print(f"Failed to download image. Status code: {response.status_code}")

    return

def get_originator():
    # Get the frame of the caller (1 level up in the stack)
    caller_frame = inspect.stack()[1]
    function_name = caller_frame.function
    module = inspect.getmodule(caller_frame.frame)
    module_name = module.__name__ if module else "Unknown Module"
    originator = f"{module_name} - {function_name}"
    
    return originator

def send_email(email_type="info", email_message="Message", originator="", attachment=None, email_recipient=None):
    
    load_dotenv()

    if not originator or originator == "":
        originator = get_originator()

    try:
        smtp_server = os.getenv("SMTP_SERVER")
        smtp_port = os.getenv("SMTP_PORT")
        smtp_user = os.getenv("SMTP_USER")
        smtp_pass = os.getenv("SMTP_PASS")
        default_recipient = os.getenv("ERROR_EMAIL_RECIPIENT")

         # Handle multiple recipients
        if email_recipient is None:
            recipients = [default_recipient]
        elif isinstance(email_recipient, str):
            recipients = [email_recipient]
        elif isinstance(email_recipient, list):
            recipients = email_recipient
        else:
            raise ValueError("email_recipient must be a string, list, or None")

        # Email content
        message = MIMEMultipart()
        message["From"] = smtp_user
        message["To"] = ", ".join(recipients)
        if email_type == "error":
            email_subject = f"Error Notification from {originator} {date.today()}"
        else:
            email_subject = f"Info Notification from {originator} {date.today()}"
        message["Subject"] = email_subject
        
        body = f"Message from the application:\n\n{email_message}"
        message.attach(MIMEText(body, "plain"))

        # Attach file if provided
        if attachment:
            if isinstance(attachment, str) and os.path.isfile(attachment):
                # If attachment is a file path
                with open(attachment, "rb") as file:
                    part = MIMEApplication(file.read(), Name=os.path.basename(attachment))
                part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment)}"'
                message.attach(part)
            elif hasattr(attachment, 'read'):
                # If attachment is a file-like object
                part = MIMEApplication(attachment.read())
                part['Content-Disposition'] = f'attachment; filename="attachment"'
                message.attach(part)
            else:
                # If attachment is some other object, convert to string
                part = MIMEApplication(str(attachment).encode())
                part['Content-Disposition'] = f'attachment; filename="attachment.txt"'
                message.attach(part)

        # Send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Secure the connection
            server.login(smtp_user, smtp_pass)
            response=server.sendmail(smtp_user, recipients, message.as_string())
            # xlogger(response)
    except Exception as e:
        rfplogger(e)
        print(f"Error sending email: {e}")

def send_email_with_credentials(smtp_server, smtp_port,smtp_user, smtp_pass, email_recipient, email_subject = "Subject", email_message="Message", email_html_message="", originator=""):
    
    if not originator or originator == "":
        originator = get_originator()

    try:
        # Email content
        message = MIMEMultipart("alternative")
        message["From"] = smtp_user
        message["To"] = email_recipient
        message["Subject"] = email_subject

        if email_message:
            message.attach(MIMEText(email_message, "plain"))
        if email_html_message:
            # Create and attach HTML content
            html_content = MIMEText(email_html_message, "html")
            message.attach(html_content)

        # Send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Secure the connection
            server.login(smtp_user, smtp_pass)
            response=server.sendmail(smtp_user, email_recipient, message.as_string())
            print("Email sent successfully.")
            return CustomResponse(data=response, status_code=200)
        
    except Exception as e:
        errormessage=f"Error sending email: {e}"
        rfplogger(errormessage)
        print(errormessage)
        return CustomResponse(data=errormessage, status_code=400)
    
def fetch_products_from_json_feed(url):
    """
    Fetches products from a JSON feed URL.
    """
    response = requests.get(url)
    response.raise_for_status()  # Raises an error for bad responses
    return response.json()

def main():
    # Example usage of send_email function
    try:
        # Test sending an email with a simple message
        response = get_originator() 
        print(f"Originator: {response}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()