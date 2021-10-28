# -*- coding: utf-8 -*-
import FunctionsHDFS
import subprocess
# import keyboard
from sys import argv

script, server, port, user = argv 

# print("Приветственное сообщение")

# command = subprocess.run(["whoami"], stdout=subprocess.PIPE, text=True)
# user = command.stdout

print("Имя пользователя HDFS: "+ user)

# history = dict()
# i = 0

while True:
    messeges = input("[" + user + "]$ ").split()
    
    # i += 1
    # history.update({i:" ".join(messeges)})
    # print(history)

    # keyboard.add_hotkey("Вверх", print("Вверх"))

    if messeges[0] == "mkdir":
        FunctionsHDFS.mkdir(messeges[1])
    elif messeges[0] == "put":
        FunctionsHDFS.put(messeges[1], server, port, user)
    elif messeges[0] == "get":
        FunctionsHDFS.get(messeges[1], server, port, user)
    elif messeges[0] == "append":
        FunctionsHDFS.append(messeges[1])
    elif messeges[0] == "delete":
        FunctionsHDFS.delete(messeges[1])
    elif messeges[0] == "ls":
        FunctionsHDFS.ls(server, port, user)
    elif messeges[0] == "cd":
        FunctionsHDFS.cd(messeges[1], server, port, user)
    elif messeges[0] == "lls":
        FunctionsHDFS.lls()
    elif messeges[0] == "lcd":
        FunctionsHDFS.lcd(messeges[1])
    elif messeges[0] == "help":
        FunctionsHDFS.help()
    else:
        print("Неизвестная команда")
