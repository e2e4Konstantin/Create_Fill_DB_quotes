import sys
from files_features.console_colors import console_colors


def output_message(text_red: str = None, text_yellow: str = None):
    """ Выводит в консоль сообщение об ошибке. """
    show_red = f"{console_colors['RED']}{text_red}{console_colors['RESET']}"
    show_yellow = f"{console_colors['YELLOW']}{text_yellow}{console_colors['RESET']}"
    print(f"{show_red}:\n\t-->> {show_yellow}")


def output_message_exit(text_red: str, text_yellow: str):
    """ Выводит в консоль сообщение об ошибке при чтении файла.
        Завершает приложение. """
    output_message(text_red, text_yellow)
    sys.exit()

if __name__ == "__main__":
    output_message("red", "yellow")
    output_message_exit("red", "yellow")