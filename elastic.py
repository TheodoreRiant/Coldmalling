import csv
import mysql.connector
from ElasticEmail.apis.tags import emails_api
from tqdm import tqdm
from more_itertools import chunked
import ElasticEmail
from ElasticEmail.model.email_recipient import EmailRecipient
from ElasticEmail.model.email_message_data import EmailMessageData
from ElasticEmail.model.email_content import EmailContent
from ElasticEmail.model.body_part import BodyPart
from ElasticEmail.model.body_content_type import BodyContentType

# Database connection details
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'rout',
    'database': 'vadato_contacts'
}

# Email sending details
# Ismérie Mail 3
html_filename = 'messages/ismerie/Acquisition_1_3.html'
txt_filename = 'messages/ismerie/Acquisition_1_3.txt'
mail_subject = 'Découvrez le réseau Angelus !'
mail_sender = 'isabelle@mission-ismerie.info'
name_sender = 'Isabelle Ismérie'

# Inco NL juillet (count : 50k)
# html_filename = 'messages/inco/NL_juillet.html'
# txt_filename = 'messages/inco/NL_juillet.txt'
# mail_subject = 'Saint-Etienne : les agresseurs du candidat RN sont liés !'
# mail_sender = 'letter@lincorrect.fr'
# name_sender = 'l\'Incorrect'
skip = 100000

# Defining the host is optional and defaults to https://api.elasticemail.com/v4
configuration = ElasticEmail.Configuration()

# Configure API key authorization: apikey
configuration.api_key['apikey'] = '4B784C5499D05AA5885262D5F93C7A3ED5AB8530C6B274DFE0347685571347A2E617F61E3BBC208F79BE8DC5E655F0C1'

# Retrieve emails from the database
conn = mysql.connector.connect(**db_config)
cursor = conn.cursor()

# Définir la campagne et le type d'événement que vous souhaitez filtrer
campagne_value = 'acquisition_1_3'
eventtype_value = 'Opened'  # Par exemple, vous pouvez changer à 'Sent', 'Clicked', etc.

# Requête SQL pour récupérer les e-mails
select_query = f"""
SELECT DISTINCT t2.email
FROM theo2 t2
LEFT JOIN ismerie_02_08_2024 t1 ON t2.email = t1.`COL 1`
WHERE t1.`COL 1` IS NULL;
"""

# select_query = f"""
# SELECT DISTINCT `to` AS email
# FROM elasticmail_result
# WHERE `fromemail` like ('%ismerie%')
#   AND eventtype in ('Clicked', 'Opened');
# """
print(f"query : {select_query}")

cursor.execute(select_query)

# Fetch all results
emails = cursor.fetchall()

# emails = [['rupierre@vadato.io']]
# Close the connection
cursor.close()
conn.close()

# Create recipient list
recipients = [
    EmailRecipient(
        Email=email[0],
    )
    for email in emails
]

# Read email content
with open(html_filename, 'r', encoding='utf-8') as html_file:
    html_content = html_file.read()

with open(txt_filename, 'r', encoding='utf-8') as txt_file:
    txt_content = txt_file.read()

# Function to send bulk emails
def send_bulk_emails(recipient_batch):
    with ElasticEmail.ApiClient(configuration) as api_client:
        api_instance = emails_api.EmailsApi(api_client)
        email_message_data = EmailMessageData(
            Recipients=recipient_batch,
            Content=EmailContent(
                body=[
                    BodyPart(
                        ContentType=BodyContentType("HTML"),
                        Content=html_content,
                        Charset="utf-8",
                    ),
                    BodyPart(
                        ContentType=BodyContentType("PlainText"),
                        Content=txt_content,
                        Charset="utf-8",
                    ),
                ],
                From=f"{name_sender} <{mail_sender}>",
                ReplyTo=mail_sender,
                Subject=mail_subject,
            ),
        )

        try:
            api_response = api_instance.emails_post(body=email_message_data, content_type='application/json', skip_deserialization=True)
            if api_response.response.status != 200:
                print(f"Error : {api_response.response.status}")
        except ElasticEmail.ApiException as e:
            print("Exception when calling EmailsApi->emails_post: %s\n" % e)

# Divide the list of recipients into batches of 1000
batches = list(chunked(recipients, 1000))

# Send the emails in batches with a progress bar
for batch in tqdm(batches, desc="Sending emails", unit="batch"):
    send_bulk_emails(batch)

print("Emails have been successfully sent.")
