import requests

def slack_push_mes(start_num, end_num, mes):
    url = 'https://hooks.slack.com/services/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'
    req = requests.post(url, json={
        'text': f'Solana chain extract failed: {mes} {start_num}0000 to {end_num}9999. Please check it out \n <@U02HSQQDX71> <@U010F3Z5Z98>'})
    print(req)

def slack_push_exception(mes):
    url = 'https://hooks.slack.com/services/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'
    req = requests.post(url, json={
        'text': mes})