import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from logging_setup import get_logger

logger = get_logger("notifier")

class Notifier:
    """Handles email notifications for pipeline events."""
    
    def __init__(self, config):
        self.enabled = config.getboolean('gmail', 'enabled', fallback=False)
        self.sender = config.get('gmail', 'sender_email', fallback='')
        self.password = config.get('gmail', 'sender_password', fallback='')
        self.recipient = config.get('gmail', 'recipient_email', fallback='')
        self.threshold = config.getint('notification', 'notification_threshold', fallback=5)
        self.interval = config.getint('notification', 'notification_interval_minutes', fallback=60)
        
        self.last_notification = None
        self.completion_count = 0
    
    def should_notify(self):
        """Check if notification should be sent based on threshold and interval."""
        if not self.enabled:
            return False
        
        if self.completion_count < self.threshold:
            return False
        
        if self.last_notification is None:
            return True
        
        elapsed = datetime.now() - self.last_notification
        return elapsed > timedelta(minutes=self.interval)
    
    def send_email(self, subject, body):
        """Send email notification."""
        if not self.enabled:
            return False
        
        try:
            msg = MIMEMultipart()
            msg['From'] = self.sender
            msg['To'] = self.recipient
            msg['Subject'] = f"ORCA Pipeline: {subject}"
            
            msg.attach(MIMEText(body, 'plain'))
            
            with smtplib.SMTP('smtp.gmail.com', 587) as server:
                server.starttls()
                server.login(self.sender, self.password)
                server.send_message(msg)
            
            logger.info(f"Email sent: {subject}")
            return True
        except Exception as e:
            logger.error(f"Failed to send email: {e}")
            return False
    
    def notify_completion(self, job_id, molecule_name):
        """Notify about job completion."""
        self.completion_count += 1
        
        if self.should_notify():
            subject = f"{self.completion_count} calculations completed"
            body = f"Latest completion: {molecule_name} (Job: {job_id})\n"
            body += f"Total completions in this batch: {self.completion_count}\n"
            body += f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            if self.send_email(subject, body):
                self.last_notification = datetime.now()
                self.completion_count = 0
    
    def notify_error(self, job_id, molecule_name, error_msg):
        """Notify about job error."""
        subject = f"Calculation failed: {molecule_name}"
        body = f"Job ID: {job_id}\n"
        body += f"Molecule: {molecule_name}\n"
        body += f"Error: {error_msg}\n"
        body += f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        self.send_email(subject, body)
    
    def notify_fatal_error(self, error_msg):
        """Notify about fatal pipeline error."""
        subject = "FATAL: Pipeline Error"
        body = f"The ORCA pipeline encountered a fatal error:\n\n"
        body += f"{error_msg}\n\n"
        body += f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        self.send_email(subject, body)
