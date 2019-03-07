import argparse


class ArgParser:

    @staticmethod
    def general_arg_parser_list() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="Search Engine Revenue")
        parser.add_argument("--source", required=True, help="source path")
        parser.add_argument("--target", required=True, help="target path")
        parser.add_argument("--app_name", required=False, default="Search Engine Revenue", help="Search Engine Revenue")

        return parser
