import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import json
import shutil
import os
from datetime import datetime


def zip_folder(folder_path, zip_filename):
    # Create a zip file from the specified folder
    shutil.make_archive(zip_filename, 'zip', folder_path)
    return zip_filename + '.zip'


def send_email(attachment_filename, attachment_display_name):

    # Get the credentials stored:
    with open("D:\\Item_replenishment_report_automation\\Credentials.json", "r+") as crednt:
        data = json.load(crednt)
        password = data["password"]

    try:
        # Credentials for usage
        sender_email = "Bcs.notifications@building-controls.com"  # Outlook email address
        sender_password = password  
        receiver_emails = ["mithul.murugaadev@building-controls.com", "jason.bail@building-controls.com"]  # List of recipient email addresses
        subject = 'Api data parsed'
        body = """Hi Jason,
        
A report of data that has been pulled through postman api is generated and shared through this automated mail. Please find the ZIP file attached.
                
Regards,
Mithul
                """

        # Set up the MIME
        message = MIMEMultipart()
        message['From'] = sender_email
        message['To'] = ', '.join(receiver_emails)
        message['Subject'] = subject

        # Attach the body
        message.attach(MIMEText(body, 'plain'))

        # Open the ZIP file to be sent
        with open(attachment_filename, 'rb') as attachment:
            # Add file as application/octet-stream
            # Email client can usually download this automatically as attachment
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())

        # Encode file in ASCII characters to send by email
        encoders.encode_base64(part)

        # Add header as key/value pair to attachment part
        part.add_header('Content-Disposition', f'attachment; filename="{attachment_display_name}"')

        # Add attachment to message and convert message to string
        message.attach(part)
        text = message.as_string()

        # Log in to SMTP server (for Outlook)
        server = smtplib.SMTP('smtp.office365.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)

        # Send email
        server.sendmail(sender_email, receiver_emails, text)

        # Close the SMTP server
        server.quit()

        return True

    except Exception as e:
        raise ValueError(f'Failed to send email: {e}')



def sender(folder_path):
    current_time = datetime.now()
    day = current_time.day
    month =  current_time.strftime("%b")
    year = current_time.year

    #folder_path = f"D:\\Replenishment_reports\\Replenishment_reports_{day}_{month}_{year}"
    zip_filename = "API_parsed"
    zip_filepath = zip_folder(folder_path, zip_filename)
    attachment_display_name = f"API_data_parsed_{day}_{month}_{year}.zip"
    send_email(zip_filepath, attachment_display_name)