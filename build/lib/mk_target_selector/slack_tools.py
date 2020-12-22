import os
import slacker

def notify_slack(message, channel='#alerts'):
    """Publishes message to slack channel

    Parameters:
        message (str):
            the message to send
        channel (str):
            the chat to send to (starts with # if a channel)

    Returns:
        None

    Examples:
        >>> notify_slack('SKA is on fire!!!')
        >>> notify_slack('Found aliens!', '#listen')
    """
    link = 'https://media2.fdncms.com/sacurrent/imager/u/original/2479890/aliens-guy.jpg'
    attachments = [{"title": "aliens", "image_url": link}]
    token = os.environ['SLACK_TOKEN']
    slack = slacker.Slacker(token)
    slack.chat.post_message(channel, message, attachments = attachments)
