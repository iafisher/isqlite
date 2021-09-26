import re

_snake_case_pattern = re.compile(r"[a-z][A-Z]")


def snake_case(s):
    name = _snake_case_pattern.sub(_snake_case_replacer, s)
    return name.lower()


def _snake_case_replacer(match):
    text = match.group(0)
    return text[0] + "_" + text[1]


_camel_case_pattern = re.compile(r"._[a-z]")


def camel_case(s):
    return _camel_case_pattern.sub(_camel_case_replacer, s)


def _camel_case_replacer(match):
    text = match.group(0)
    return text[0] + text[2].upper()


class StringBuilder:
    def __init__(self):
        self.parts = []

    def text(self, s):
        self.parts.append(s)

    def build(self):
        return "".join(self.parts)