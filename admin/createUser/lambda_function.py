import os
import json
import boto3
import random
import string

iam_client = boto3.client('iam')
ses_client = boto3.client('ses', region_name='ap-southeast-2')

def generate_password():
    upper = [random.choice(string.ascii_uppercase) for _ in range(3)]
    lower = [random.choice(string.ascii_lowercase) for _ in range(3)]
    digits = [random.choice(string.digits) for _ in range(3)]
    sym = [random.choice(string.punctuation) for _ in range(3)]
    password = upper + lower + digits + sym
    random.shuffle(password)
    password = ''.join(password)
    return password

def lambda_handler(event, context):
    try:
        user_name = event['userName']
        user_email = event['userEmail']
        
        # Create IAM user
        iam_client.create_user(UserName=user_name)
        
        # Generate a temporary password
        temp_password = generate_password()
        
        # Create a login profile for the user with the temporary password
        iam_client.create_login_profile(
            UserName=user_name,
            Password=temp_password,
            PasswordResetRequired=True
        )
        
        # Attach the IAMUserChangePassword policy to the user
        iam_client.attach_user_policy(
            UserName=user_name,
            PolicyArn='arn:aws:iam::aws:policy/IAMUserChangePassword'
        )
        
        # Grant read-only access to billing information
        iam_client.attach_user_policy(
            UserName=user_name,
            PolicyArn='arn:aws:iam::aws:policy/AWSBillingReadOnlyAccess'
        )
        
        # Attach the custom IAMFullAccessOwnUser policy to the user
        iam_client.attach_user_policy(
            UserName=user_name,
            PolicyArn='arn:aws:iam::339713004220:policy/IAMFullAccessOwnUser'
        )
    
        # Compose the email
        subject = 'Your new IAM user account'
        body_text = f"""Hi,
    
Your IAM user account has been created.
You can sign in at: https://data14group1.signin.aws.amazon.com/console
Your username: {user_name}
Your temporary password: {temp_password}
Please log in, reset your password, and enable Multi-Factor Authentication (MFA) for added security.

Regards,
Sam"""
        
        response = ses_client.send_email(
            Source=os.getenv('SOURCE_EMAIL'),
            Destination={
                'ToAddresses': [
                    user_email,
                ],
            },
            Message={
                'Subject': {
                    'Data': subject,
                    'Charset': 'UTF-8'
                },
                'Body': {
                    'Text': {
                        'Data': body_text,
                        'Charset': 'UTF-8'
                    }
                }
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps('User created and email sent successfully!')
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(str(e))
        }
