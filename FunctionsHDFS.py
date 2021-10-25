# -*- coding: utf-8 -*-
import subprocess
import os

"""Cоздание каталога в HDFS"""
def mkdir (nameCatalog):
    print(nameCatalog)

"""Загрузка/создание файла в HDFS"""
def put (pathLocalFile, localhost, port, user):
    # Шаг 1 Запрос на подтверждение
    # curl -i -X PUT "http://localhost:50070/webhdfs/v1/tmp/file01?user.name=oioopcaxix&op=CREATE&overwrite=true"
    
    fileList = pathLocalFile.split('/')
    nameLocalFile = fileList[-1]
    print(nameLocalFile)

    link = "http://" + localhost + ":" + port +"/webhdfs/v1/user/" + user + "/"+ nameLocalFile +"?user.name=" + user + "&op=CREATE&overwrite=true"
    command = subprocess.run(["curl","-i","-X","PUT",link],stdout=subprocess.PIPE, text=True)
    answer = command.stdout.split()
    location = ""
    for i in range(len(answer)):
        if answer[i]=="Location:":
            location = answer[i+1]
    
    print(location)
    # Шау 2
    # curl -i -X PUT -T ~/mysources/WordCount/file01 "http://localhost:50075/webhdfs/v1/tmp/file01?op=CREATE&user.name=oioopcaxix&namenoderpcaddress=localhost:9000&createflag=&createparent=true&overwrite=true"
    command2 = subprocess.run(["curl","-i","-X","PUT","-t",pathLocalFile,"%s" % location],stdout=subprocess.PIPE, text=True)
    a = command2.stdout
    print(a)


"""Cкачивание файла из HDFS"""
def get (nameFile):
    pass

"""Kонкатенация файла в HDFS с локальным файлом"""
def append (nameLocalFile):
    pass

"""Удаление файла в HDFS"""
def delete (nameFile):
    pass

"""Отображение содержимого текущего каталога в HDFS с разделением файлов и каталогов"""
def ls ():
    pass

"""Переход в другой каталог в HDFS"""
def cd (nameCatalog):
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