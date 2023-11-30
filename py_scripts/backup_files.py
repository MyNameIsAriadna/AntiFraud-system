#!/usr/bin/python3
import os

keywords = ['passport_blacklist', 'terminals', 'transactions']
# Формируем путь к исходному файлу
for file in os.listdir("/home/project/"):
    for keyword in keywords:
        if keyword in file:
            source_file = "/home/project/" + file   
            
# Формируем новый путь и имя файла с расширением ".backup" в папке archive        
            backup_file = "/home/project/archive/" + file + ".backup" 
            
# Переименовываем и перемещаем файл         
            os.rename(source_file, backup_file)