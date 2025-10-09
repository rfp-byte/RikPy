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

def send_email(email_type="info", email_message="Message", originator="", attachment=None, email_recipient=None, timeout=30, max_retries=3):
    """
    Send email with improved error handling, validation, and retry logic.
    
    Args:
        email_type: Type of email ("info" or "error")
        email_message: Email message content
        originator: Origin of the email (auto-detected if empty)
        attachment: File attachment (path, file-like object, or data)
        email_recipient: Recipient email(s) - string, list, or None for default
        timeout: SMTP timeout in seconds (default: 30)
        max_retries: Maximum retry attempts (default: 3)
    
    Returns:
        CustomResponse: Success/failure response with details
    """
    
    load_dotenv()

    if not originator or originator == "":
        originator = get_originator()

    # Validate environment variables
    smtp_server = os.getenv("SMTP_SERVER")
    smtp_port = os.getenv("SMTP_PORT")
    smtp_user = os.getenv("SMTP_USER")
    smtp_pass = os.getenv("SMTP_PASS")
    default_recipient = os.getenv("ERROR_EMAIL_RECIPIENT")

    if not all([smtp_server, smtp_port, smtp_user, smtp_pass, default_recipient]):
        missing_vars = []
        if not smtp_server: missing_vars.append("SMTP_SERVER")
        if not smtp_port: missing_vars.append("SMTP_PORT")
        if not smtp_user: missing_vars.append("SMTP_USER")
        if not smtp_pass: missing_vars.append("SMTP_PASS")
        if not default_recipient: missing_vars.append("ERROR_EMAIL_RECIPIENT")
        
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        rfplogger(error_msg)
        return CustomResponse(data=error_msg, status_code=400)

    # Validate and convert port to integer
    try:
        smtp_port = int(smtp_port)
    except (ValueError, TypeError):
        error_msg = f"Invalid SMTP_PORT: {smtp_port}. Must be a valid integer."
        rfplogger(error_msg)
        return CustomResponse(data=error_msg, status_code=400)

    # Handle multiple recipients
    try:
        if email_recipient is None:
            recipients = [default_recipient]
        elif isinstance(email_recipient, str):
            recipients = [email_recipient]
        elif isinstance(email_recipient, list):
            recipients = email_recipient
        else:
            raise ValueError("email_recipient must be a string, list, or None")
    except ValueError as e:
        error_msg = f"Invalid email_recipient: {e}"
        rfplogger(error_msg)
        return CustomResponse(data=error_msg, status_code=400)

    # Validate recipients
    for recipient in recipients:
        if not recipient or '@' not in recipient:
            error_msg = f"Invalid email recipient: {recipient}"
            rfplogger(error_msg)
            return CustomResponse(data=error_msg, status_code=400)

    # Retry logic
    last_error = None
    for attempt in range(max_retries):
        try:
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
                try:
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
                except Exception as e:
                    error_msg = f"Error processing attachment: {e}"
                    rfplogger(error_msg)
                    return CustomResponse(data=error_msg, status_code=400)

            # Send email with timeout
            with smtplib.SMTP(smtp_server, smtp_port, timeout=timeout) as server:
                server.starttls()  # Secure the connection
                server.login(smtp_user, smtp_pass)
                response = server.sendmail(smtp_user, recipients, message.as_string())
                
            # Success - log and return
            success_msg = f"Email sent successfully to {', '.join(recipients)} (attempt {attempt + 1})"
            rfplogger(success_msg)
            return CustomResponse(data={"message": success_msg, "recipients": recipients, "attempt": attempt + 1}, status_code=200)
            
        except smtplib.SMTPAuthenticationError as e:
            error_msg = f"SMTP Authentication failed: {e}"
            rfplogger(error_msg)
            return CustomResponse(data=error_msg, status_code=401)  # Don't retry auth errors
            
        except smtplib.SMTPRecipientsRefused as e:
            error_msg = f"SMTP Recipients refused: {e}"
            rfplogger(error_msg)
            return CustomResponse(data=error_msg, status_code=400)  # Don't retry recipient errors
            
        except smtplib.SMTPException as e:
            last_error = f"SMTP Error (attempt {attempt + 1}/{max_retries}): {e}"
            rfplogger(last_error)
            if attempt < max_retries - 1:
                import time
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
                
        except Exception as e:
            last_error = f"Unexpected error (attempt {attempt + 1}/{max_retries}): {e}"
            rfplogger(last_error)
            if attempt < max_retries - 1:
                import time
                time.sleep(2 ** attempt)  # Exponential backoff
                continue

    # All retries failed
    final_error = f"Failed to send email after {max_retries} attempts. Last error: {last_error}"
    rfplogger(final_error)
    return CustomResponse(data=final_error, status_code=500)

def send_email_with_credentials(smtp_server, smtp_port, smtp_user, smtp_pass, email_recipient, email_subject="Subject", email_message="Message", email_html_message="", originator="", timeout=30, max_retries=3):
    """
    Send email with explicit credentials and improved error handling.
    
    Args:
        smtp_server: SMTP server address
        smtp_port: SMTP server port
        smtp_user: SMTP username
        smtp_pass: SMTP password
        email_recipient: Recipient email address
        email_subject: Email subject
        email_message: Plain text message
        email_html_message: HTML message (optional)
        originator: Origin of the email (auto-detected if empty)
        timeout: SMTP timeout in seconds (default: 30)
        max_retries: Maximum retry attempts (default: 3)
    
    Returns:
        CustomResponse: Success/failure response with details
    """
    
    if not originator or originator == "":
        originator = get_originator()

    # Validate input parameters
    if not all([smtp_server, smtp_port, smtp_user, smtp_pass, email_recipient]):
        missing_params = []
        if not smtp_server: missing_params.append("smtp_server")
        if not smtp_port: missing_params.append("smtp_port")
        if not smtp_user: missing_params.append("smtp_user")
        if not smtp_pass: missing_params.append("smtp_pass")
        if not email_recipient: missing_params.append("email_recipient")
        
        error_msg = f"Missing required parameters: {', '.join(missing_params)}"
        rfplogger(error_msg)
        return CustomResponse(data=error_msg, status_code=400)

    # Validate port
    try:
        smtp_port = int(smtp_port)
    except (ValueError, TypeError):
        error_msg = f"Invalid smtp_port: {smtp_port}. Must be a valid integer."
        rfplogger(error_msg)
        return CustomResponse(data=error_msg, status_code=400)

    # Validate email recipient
    if '@' not in email_recipient:
        error_msg = f"Invalid email recipient: {email_recipient}"
        rfplogger(error_msg)
        return CustomResponse(data=error_msg, status_code=400)

    # Retry logic
    last_error = None
    for attempt in range(max_retries):
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

            # Send email with timeout
            with smtplib.SMTP(smtp_server, smtp_port, timeout=timeout) as server:
                server.starttls()  # Secure the connection
                server.login(smtp_user, smtp_pass)
                response = server.sendmail(smtp_user, email_recipient, message.as_string())
                
            # Success - log and return
            success_msg = f"Email sent successfully to {email_recipient} (attempt {attempt + 1})"
            rfplogger(success_msg)
            return CustomResponse(data={"message": success_msg, "recipient": email_recipient, "attempt": attempt + 1}, status_code=200)
            
        except smtplib.SMTPAuthenticationError as e:
            error_msg = f"SMTP Authentication failed: {e}"
            rfplogger(error_msg)
            return CustomResponse(data=error_msg, status_code=401)  # Don't retry auth errors
            
        except smtplib.SMTPRecipientsRefused as e:
            error_msg = f"SMTP Recipients refused: {e}"
            rfplogger(error_msg)
            return CustomResponse(data=error_msg, status_code=400)  # Don't retry recipient errors
            
        except smtplib.SMTPException as e:
            last_error = f"SMTP Error (attempt {attempt + 1}/{max_retries}): {e}"
            rfplogger(last_error)
            if attempt < max_retries - 1:
                import time
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
                
        except Exception as e:
            last_error = f"Unexpected error (attempt {attempt + 1}/{max_retries}): {e}"
            rfplogger(last_error)
            if attempt < max_retries - 1:
                import time
                time.sleep(2 ** attempt)  # Exponential backoff
                continue

    # All retries failed
    final_error = f"Failed to send email after {max_retries} attempts. Last error: {last_error}"
    rfplogger(final_error)
    return CustomResponse(data=final_error, status_code=500)
    
def fetch_products_from_json_feed(url):
    """
    Fetches products from a JSON feed URL.
    """
    response = requests.get(url)
    response.raise_for_status()  # Raises an error for bad responses
    return response.json()

def main():
    # Example usage of improved send_email function
    try:
        # Test sending an email with improved error handling
        response = send_email(
            email_type="info",
            email_message="This is a test message from the improved email function",
            email_recipient="test@example.com"  # Replace with actual email
        )
        
        if response.status_code == 200:
            print(f"✅ Email sent successfully: {response.data}")
        else:
            print(f"❌ Email failed: {response.data}")
            
        # Test with environment variables (recommended approach)
        response2 = send_email(
            email_type="error",
            email_message="This is an error notification",
            # email_recipient=None  # Will use ERROR_EMAIL_RECIPIENT from .env
        )
        
        if response2.status_code == 200:
            print(f"✅ Error email sent successfully: {response2.data}")
        else:
            print(f"❌ Error email failed: {response2.data}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()