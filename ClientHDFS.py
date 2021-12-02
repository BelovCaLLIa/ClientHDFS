# -*- coding: utf-8 -*-
import sys
import FunctionsHDFS
import subprocess
from sys import argv
# import ftplib

script, server, port, user = argv 
# server, port, user = "localhost", "50070", "oioopcaxix"

# print("Приветственное сообщение")

# command = subprocess.run(["whoami"], stdout=subprocess.PIPE, text=True)
# user = command.stdout

# FunctionsHDFS.pathChange(user, 1)
# Задаём начальный путь
FunctionsHDFS.setPath("user")
FunctionsHDFS.setPath(f"{user}")

while True:
    messeges = input("[" + user + "]$ ").split()        

    # keyboard.add_hotkey("Вверх", print("Вверх"))
    if len(messeges) == 0:
        continue
    elif messeges[0] == "mkdir":
        FunctionsHDFS.mkdir(messeges[1], server, port, user)
    elif messeges[0] == "put":
        FunctionsHDFS.put(messeges[1], server, port, user)
    elif messeges[0] == "get":
        FunctionsHDFS.get(messeges[1], server, port, user)
    elif messeges[0] == "append":
        FunctionsHDFS.append(messeges[1], server, port, user)
    elif messeges[0] == "delete":
        FunctionsHDFS.delete(messeges[1], server, port, user)
    elif messeges[0] == "ls":
        FunctionsHDFS.ls(server, port, user, "print")
    elif messeges[0] == "cd":
        FunctionsHDFS.cd(messeges[1], server, port, user)
    elif messeges[0] == "lls":
        FunctionsHDFS.lls()
    elif messeges[0] == "lcd":
        FunctionsHDFS.lcd(messeges[1])
    elif messeges[0] == "help":
        FunctionsHDFS.help()
    elif messeges[0] == "exit" or messeges[0] == "e":
        sys.exit()
    else:
        print("Неизвестная команда")
