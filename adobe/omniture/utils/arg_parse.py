import argparse


class ArgParser:

    @staticmethod
    def general_arg_parser_list() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="Search Engine Revenue")
        parser.add_argument("--source", required=False, help="s3 source url")
        parser.add_argument("--app_name", required=True, help="Search Engine Revenue")

        return parser
