from craco.craco_run.slackpost import SlackPostManager
import argparse

def main():
    slackbot = SlackPostManager(channel=args.channel_id)
    if args.attachment is not None:
        slackbot.upload_file(args.attachment, args.message)
    else:
        slackbot.post_message(args.message)


if __name__ == '__main__':
    a = argparse.ArgumentParser()
    a.add_argument("message", type=str,help="Your slack message")
    a.add_argument("-cid", "--channel-id", type=str, help="Id of the slack-channel (def:C06FCTQ6078 / #craco-operations)", default="C06FCTQ6078")
    a.add_argument("-att", "--attachment",  type=str, help="Path to any attachment file you may want to give", default=None)
    args = a.parse_args()
    main()
