# -*- coding: utf-8 -*-
import subprocess
import os

"""Показывает путь до текущей папки"""
def pwd():
    command = subprocess.run(["pwd"],stdout=subprocess.PIPE, text=True)
    return command.stdout()

"""Выведенный текст меняет цвет"""
def yellowText(text):
    print("\033[36m {}" .format(text))

"""Выведенный текст возвращается и исходное значение"""
def defaltColorText(text):
    print("\033[0m {}".format(text))

"""Cоздание каталога в HDFS"""
def mkdir (nameCatalog, localhost, port, user):
    # Пример
    # curl -i -X PUT "http://localhost:50070/webhdfs/v1/user/aslebedev/dir?user.name=aslebedev&op=MKDIRS"
    link = "http://" + localhost + ":" + port + "/webhdfs/v1/user/" + user + "/" + nameCatalog + "?user.name=" + user + "&op=MKDIRS"
    command = subprocess.run(["curl","-i","-X","PUT",link],stdout=subprocess.PIPE, text=True)
    
    print(command.stdout)

"""Загрузка/создание существующего файла в HDFS"""
def put (pathLocalFile, localhost, port, user):
    
    fileList = pathLocalFile.split('/')
    nameLocalFile = fileList[-1]

    # Проверка на существования локально файла
    nameFilesDirectories = subprocess.run(["ls","-f"],stdout=subprocess.PIPE, text=True)
    nameFileList = nameFilesDirectories.stdout.split()
    count = 0
    for i in range(len(nameFileList)):
        if nameLocalFile == nameFileList[i]:
            count += 1
    
    if count == 0:
        print("Нет такого локального файла")
        return
        

    # Шаг 1 Запрос на подтверждение
    # Пример
    # curl -i -X PUT "http://localhost:50070/webhdfs/v1/tmp/file01?user.name=oioopcaxix&op=CREATE&overwrite=true"
    link = "http://" + localhost + ":" + port +"/webhdfs/v1/user/" + user + "/"+ nameLocalFile +"?user.name=" + user + "&op=CREATE&overwrite=true"
    command = subprocess.run(["curl","-i","-X","PUT",link],stdout=subprocess.PIPE, text=True)
    answer = command.stdout.split()
    location = ""
    for i in range(len(answer)):
        if answer[i]=="Location:":
            location = answer[i+1]
    
    print(location)
    # Шау 2
    # Пример
    # curl -i -X PUT -T ~/mysources/WordCount/file01 "http://localhost:50075/webhdfs/v1/tmp/file01?op=CREATE&user.name=oioopcaxix&namenoderpcaddress=localhost:9000&createflag=&createparent=true&overwrite=true"
    command2 = subprocess.run(["curl","-i","-X","PUT","-t",pathLocalFile,"%s" % location],stdout=subprocess.PIPE, text=True)
    
    print(command2.stdout)


"""Cкачивание файла из HDFS"""
def get (nameFile):
    pass

"""Kонкатенация файла в HDFS с локальным файлом"""
def append (nameLocalFile):
    pass

"""Удаление файла в HDFS"""
def delete (pathFile):
    # Пример
    # curl -i -X DELETE "http://<host>:<port>/webhdfs/v1/<path>?op=DELETE[&recursive=<true |false>]"
    pass

"""Отображение содержимого текущего каталога в HDFS с разделением файлов и каталогов"""
def ls (localhost, port, user):
    # Удобнее использовать команду hdfs dfs -ls, но по заданию нужно использовать WebHDFS
    # Пример
    # curl -i "http://localhost:50070/webhdfs/v1/user/oioopcaxix/wordcount?user.name=oioopcaxix&op=LISTSTATUS"
    
    # а теперь нужно привезать выполнение команды ls к команде cd через переменную пути (которая будет не обязательная и по умолчанию = /user/oioopcaxix)
    link = "http://" + localhost + ":" + port + "/webhdfs/v1/user/" + user + "?user.name=" + user + "&op=LISTSTATUS"
    command = subprocess.run(["curl","-i",link],stdout=subprocess.PIPE, text=True)
    answer = command.stdout.split("\n")

    print("\n")

    count = 0
    for i in range(len(answer)):
        if answer[i] == "{\"FileStatuses\":{\"FileStatus\":[":
            count = int(i)
            break
        print(answer[i])
    
    # Разделения второй части stdout построчно
    lsList = []
    for j in range(len(answer) - count):
        lsList += answer[count + j].split(",")

    # Вывод в столбик информации о файлах
    for i in range(len(lsList)):
        # Метод find ищет в строке подстроку, если не находит возвращает -1
        if lsList[i].find("pathSuffix") != -1 or lsList[i].find("owner") != -1:
            yellowText(lsList[i])
            continue
        defaltColorText(lsList[i])


"""Переход в другой каталог в HDFS"""
def cd (pathCatalog, localhost, port, user):
    # link = ""
    # subprocess.run(["curl","-i",link])
    pass

"""Отображение содержимого текущего локального каталога с разделением файлов и каталогов"""
def lls ():
    subprocess.run(["ls","-l","--color"])

"""Переход в другой локальный каталог"""
def lcd (nameLocalCatalog):
    try:
        # Функция для смены директории
        os.chdir(nameLocalCatalog)
    except FileNotFoundError:
        # Если данного пути не существует
        print('Ошибка: Указанный путь не найден.')

"""Что умеет этот терминал"""
def help():
    print("Команда put [Путь к файлу] - Загружает файл в HDFS")

    print("Команда lls - показывает файлы в локальной папке")
    print("Команда lcd [Имя локального файла] - работает аналогично cd")