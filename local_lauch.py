import os
import json

from dotenv import load_dotenv

import lambda_function


def main():
    event = {}
    with open('events/s3.json', 'r', encoding='utf8') as f:
        json_str = f.read()
        event = json.loads(json_str)

    lambda_function.lambda_handler(event, None)

if __name__ == '__main__':
    load_dotenv()
    main()
