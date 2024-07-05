import argparse
import json
import traceback
import sys
import re


_global_args = {}



class RawSqlExpression:
    def __init__(self, expression):
        self.expression = expression

    def __str__(self):
        return self.expression

def needs_quoting(string: str) -> bool:
    """Determines if the string needs quoting based on specific rules.
    
    Returns True if the string needs quoting, otherwise False.
    """
    remove_quote = re.compile(r'^"(([_A-Z]+[_A-Z0-9$]*)|(\$\d+))"$')
    result = remove_quote.search(string)
    return not result

class ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise Exception("Argument Error: " + str(message))

def argument_exists(parser, arg_name):
    for action in parser._actions:
        if arg_name in action.option_strings:
            return True
    return False

def getResolvedOptions(args, options):
    parser = ArgumentParser()

    parser.add_argument('--job-bookmark-option', choices = ['job-bookmark-enable', 'job-bookmark-pause', 'job-bookmark-disable'], required = False)
    parser.add_argument('--continuation-option', choices = ['continuation-enabled', 'continuation-readonly', 'continuation-ignore'], required = False)


    parser.add_argument('--JOB_NAME', required=False)
    parser.add_argument('--JOB_ID', required=False)
    parser.add_argument('--JOB_RUN_ID', required=False)

        

    parser.add_argument('--TempDir', required=False)
    options = [opt for opt in options if opt not in {'TempDir','JOB_NAME'}]
    for option in options:
        parser.add_argument('--' + option, required=True)

    parsed, extra = parser.parse_known_args(args[1:])

    parsed_dict = vars(parsed)

    _global_args.update(parsed_dict)

    return parsed_dict
