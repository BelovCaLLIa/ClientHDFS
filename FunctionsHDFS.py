# -*- coding: utf-8 -*-
import subprocess
import os

# Переменная текущего пути
__hadoopPath = "/webhdfs/v1/"
__currentDistantRoot = 0

"""Изменение пути в Hadoop"""
def setPath(nameDirectory):
    global __hadoopPath
    global __currentDistantRoot
  
    # Разделения nameDirectory по одной
    nameDirectoryList = nameDirectory.split('/')
    distanceRoot = len(nameDirectoryList)

    if nameDirectory == "..":
        distanceRoot = -1

    pathList = __hadoopPath.split('/')

    """ # Переход на несколько каталогов вверх
    if distanceRoot > 1:
        print("distanceRoot > 1")
        __currentDistantRoot += distanceRoot
        __hadoopPath += "%s" % nameDirectory
        print(__hadoopPath) """

    # Переход на один каталог вглубь
    if distanceRoot == 1:
        # Срабатывает при первом использовании 
        if __currentDistantRoot == 0:
            __hadoopPath += nameDirectory
            __currentDistantRoot += 1
            return 200
        __currentDistantRoot += 1
        __hadoopPath += f"/{nameDirectory}"
        return 200
    # Переход выше (..)
    elif distanceRoot == -1:
        # Проверка на возможность перехода вверх
        if pathList[-2] == "v1" and pathList[-1] == "":            
            pathList.pop(-1)
            __hadoopPath = '/'.join(pathList) + '/'
            return -2
        else:
            __currentDistantRoot -= 1
            pathList.pop(-1)
            # Срабатывает при переходе откуда-то в корневой каталог
            if __currentDistantRoot == 0:
                __hadoopPath = '/'.join(pathList) + '/'
                return 200
            __hadoopPath = '/'.join(pathList)
            return 200
    
"""Возвращает текущий путь в Hadoop"""
def getPath():
    return __hadoopPath

"""Показывает путь до текущей папки"""
def pwd():
    command = subprocess.run(["pwd"],stdout=subprocess.PIPE, text=True)
    return command.stdout()

"""Выведенный текст меняет цвет"""
def blueText(text):
    print("\033[36m {}" .format(text))

"""Выведенный текст возвращается и исходное значение"""
def defaltColorText(text):
    print("\033[0m {}".format(text))


"""Cоздание каталога в HDFS"""
def mkdir (nameCatalog, server, port, user):
    pathHDFS = getPath()
    # Пример
    # curl -i -X PUT "http://localhost:50070/webhdfs/v1/user/aslebedev/dir?user.name=aslebedev&op=MKDIRS"
    link = "http://" + server + ":" + port + pathHDFS + "/" + nameCatalog + "?user.name=" + user + "&op=MKDIRS"
    command = subprocess.run(["curl","-i","-X","PUT",link],stdout=subprocess.PIPE, text=True)
    
    print(command.stdout)

"""Загрузка/создание существующего файла в HDFS"""
def put (pathLocalFile, server, port, user):
    
    fileList = pathLocalFile.split('/')
    """ for _ in range(len(fileList)):
        print(f"{_} : {fileList[_]}") """
    nameLocalFile = fileList[-1]

    # Проверка на существования локально файла
    nameFilesDirectories = subprocess.run(["ls","-f"],stdout=subprocess.PIPE, text=True)
    nameFileList = nameFilesDirectories.stdout.split()
    print(nameFileList)
    count = 0
    for i in range(len(nameFileList)):
        if nameLocalFile == nameFileList[i]:
            count += 1
    
    if count == 0:
        print("Нет такого локального файла")
        return
        
    pathHDFS = getPath()

    # Шаг 1 Запрос на подтверждение
    # Пример
    # curl -i -X PUT "http://localhost:50070/webhdfs/v1/tmp/file01?user.name=oioopcaxix&op=CREATE&overwrite=true"
    # curl -i -X PUT "http://localhost:50070/webhdfs/v1/user/oioopcaxix/test?user.name=oioopcaxix&op=CREATE&overwrite=true"
    link = "http://" + server + ":" + port + pathHDFS + "/" + nameLocalFile +"?user.name=" + user + "&op=CREATE&overwrite=true"
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
    # curl -i -X PUT -T /home/oioopcaxix/Documents/VSCode/Hadoop/Laba1/test "http://172.16.242.128:50075/webhdfs/v1/user/test?op=CREATE&user.name=oioopcaxix&namenoderpcaddress=localhost:9000&createflag=&createparent=true&overwrite=true"
    # pathLocalFile = "/home/oioopcaxix/Documents/VSCode/Hadoop/Laba1/test"
    command2 = subprocess.run(["curl","-i","-X","PUT","-T",pathLocalFile,"%s" % location],stdout=subprocess.PIPE, text=True)
    
    print(command2.stdout)


"""Cкачивание файла из HDFS"""
def get (nameFile, server, port, user):

    # Написать проверку
    
    pathHDFS = getPath()
    # curl -i -L "http://localhost:50070/webhdfs/v1/user/oioopcaxix/test?user.name=oioopcaxix&op=OPEN"
    link = "http://" + server + ":" + port + pathHDFS + "/" + nameFile +"?user.name=" + user + "&op=OPEN"
    command = subprocess.run(["curl","-i","-L",link],stdout=subprocess.PIPE, text=True)
    print(command.stdout)
    answer = command.stdout.split()

    # Выбираем из ответа текстовую часть
    text = ""
    for i in range(len(answer)):
        if answer[i]=="Content-Length:" and answer[i+1] != "0":
            i += 1
            count = len(answer) - i
            while count != 0:
                text += answer[i]
                i += 1
                count -= 1

    # Запись в файл
    fail = open(nameFile, 'w')
    try:
        fail.write(text)
    finally:
        fail.close()

"""Kонкатенация файла в HDFS с локальным файлом"""
def append (pathLocalFile, server, port, user):

    # Написать проверку
    
    fileList = pathLocalFile.split('/')
    nameLocalFile = fileList[-1]

    pathHDFS = getPath()
    # Шаг 1
    # curl -i -X POST "http://<HOST>:<PORT>/webhdfs/v1/<PATH>?op=APPEND[&buffersize=<INT>][&noredirect=<true|false>]"
    # curl -i -X POST "http://localhost:50070/webhdfs/v1/user/oioopcaxix/test?user.name=oioopcaxix&op=APPEND"
    link = "http://" + server + ":" + port + pathHDFS + "/" + nameLocalFile +"?user.name=" + user + "&op=APPEND"
    print(f"link: {link}")
    command = subprocess.run(["curl","-i","-X","POST",link],stdout=subprocess.PIPE, text=True)
    answer = command.stdout.split()
    location = ""
    for i in range(len(answer)):
        if answer[i]=="Location:":
            location = answer[i+1]
    
    print(location)

    # Шаг 2 
    # location = http://172.16.242.128:50075/webhdfs/v1/user/oioopcaxix/test?op=APPEND&user.name=oioopcaxix&namenoderpcaddress=localhost:9000
    # curl -i -X POST -T /home/oioopcaxix/Documents/VSCode/Hadoop/Laba1/test "http://172.16.242.128:50075/webhdfs/v1/user/oioopcaxix/test?op=APPEND&user.name=oioopcaxix&namenoderpcaddress=localhost:9000"
    command2 = subprocess.run(["curl","-i","-X","POST","-T",pathLocalFile,"%s" % location],stdout=subprocess.PIPE, text=True)
    
    print(command2.stdout)

"""Удаление файла в HDFS"""
def delete (pathFile, server, port, user):
    pathHDFS = getPath()
    # Пример
    # curl -i -X DELETE "http://localhost:50070/webhdfs/v1/user/oioopcaxix/test?user.name=oioopcaxix&op=DELETE[&recursive=<true |false>]"
    link = "http://" + server + ":" + port + pathHDFS + "/"+ pathFile +"?user.name=" + user + "&op=DELETE"
    subprocess.run(["curl", "-i", "-X", "DELETE", link])
    
    print('\n')

"""Отображение содержимого текущего каталога в HDFS с разделением файлов и каталогов"""
def ls (server, port, user, mode):
    # Удобнее использовать команду hdfs dfs -ls, но по заданию нужно использовать WebHDFS
    # Пример
    # curl -i "http://localhost:50070/webhdfs/v1/user/oioopcaxix/wordcount?user.name=oioopcaxix&op=LISTSTATUS"
    # path = getPath() + pathCatalog
    path = getPath()
    # Отладить и узнать, почему не отлавливается 404
    link = "http://" + server + ":" + port + path + "?user.name=" + user + "&op=LISTSTATUS"
    command = subprocess.run(["curl","-i",link],stdout=subprocess.PIPE, text=True)
    answer = command.stdout.split("\n")
    if mode == "print":
        print('\n')

    # Работа над ответом
    count = 0
    for i in range(len(answer)):
        if answer[i] == 'HTTP/1.1 404 Not Found':
            return 404
        if mode == "print":
            if answer[i] == "{\"FileStatuses\":{\"FileStatus\":[":
                count = int(i)
                break
            print(answer[i])
        elif mode == "noprint":
            print("Переход выполнен!")
            return
    
    # Разделения второй части stdout построчно
    lsList = []
    for j in range(len(answer) - count):
        lsList += answer[count + j].split(",")

    # Вывод в столбик информации о файлах
    for i in range(len(lsList)):
        # Метод find ищет в строке подстроку, если не находит возвращает -1
        if lsList[i].find("pathSuffix") != -1 or lsList[i].find("owner") != -1:
            blueText(lsList[i])
            continue
        defaltColorText(lsList[i])

"""Переход в другой каталог в HDFS"""
def cd (nameCatalog, server, port, user):
    # Проверка на существование пути
    answer = setPath(nameCatalog)
    if answer == 200:
        answer_two = ls(server, port, user, "noprint")
        if answer_two == 404:
            setPath("..")
            print("Каталога не существует")
            print(getPath())
            return
        else:
            print(getPath())
    elif answer == -2:
        print("Вы уже в корневом каталоге")
        print(getPath())
        return

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
    print("Команда delete [Имя файла] - Удаляет файл из HDFS")
    print("Команда mkdir [Название директории] - Создаёт директорию в HDFS")
    print("Команда append [Путь к файлу] - Обновляет файл в HDFS")
    print("Команда ls - показывает файлы в каталоге HDFS")
    print("Команда cd [имя директории] - показывает директории/файлы в каталоге HDFS")
    print("Команда lls - показывает файлы в локальной папке")
    print("Команда lcd [Имя локального файла] - работает аналогично cd")
    print("Команда help - Выводит список возможностей приложения")
    print("Команда exit/e - Выход из программы")