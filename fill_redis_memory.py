#!/usr/bin/env python3
import redis
import random
import string
import time
import sys

# Подключение к Redis
r = redis.Redis(
    host='localhost',
    port=6379,
    password='password',  # замените на реальный пароль
    decode_responses=True
)

def random_string(length=1024):
    """Генерирует случайную строку заданной длины"""
    letters = string.ascii_letters + string.digits
    return ''.join(random.choice(letters) for i in range(length))

def fill_memory(target_mb=90):
    """Заполняет память Redis до указанного процента от maxmemory"""
    print(f"Заполнение памяти Redis до ~{target_mb}MB...")
    
    key_counter = 0
    total_size = 0
    target_bytes = target_mb * 1024 * 1024
    
    try:
        while True:
            # Проверяем текущее использование памяти
            memory_info = r.info('memory')
            used_memory = int(memory_info['used_memory'])
            
            if used_memory >= target_bytes:
                print(f"Достигнут целевой объем памяти: {used_memory / (1024*1024):.2f}MB")
                break
            
            # Создаем ключ с данными
            key = f"test:key:{key_counter}"
            value = random_string(random.randint(500, 5000))  # 0.5-5KB
            
            # Устанавливаем ключ
            r.set(key, value)
            
            # Иногда устанавливаем TTL для некоторых ключей
            if key_counter % 5 == 0:
                r.expire(key, random.randint(60, 300))
            
            key_counter += 1
            total_size += len(key) + len(value)
            
            # Периодически выводим прогресс
            if key_counter % 100 == 0:
                progress = (used_memory / target_bytes) * 100
                print(f"Прогресс: {progress:.1f}% | Ключей: {key_counter} | Память: {used_memory / (1024*1024):.2f}MB")
                sys.stdout.flush()
                
            # Небольшая пауза для контроля
            if key_counter % 1000 == 0:
                time.sleep(0.1)
    
    except redis.exceptions.ResponseError as e:
        print(f"Ошибка Redis: {e}")
        print("Вероятно, достигнут лимит памяти и началось вытеснение ключей")
    
    except KeyboardInterrupt:
        print("\nПрервано пользователем")
    
    finally:
        print(f"\nИтог: создано {key_counter} ключей")
        memory_info = r.info('memory')
        print(f"Использовано памяти: {int(memory_info['used_memory']) / (1024*1024):.2f}MB")
        print(f"Максимальная память: {int(memory_info['maxmemory']) / (1024*1024):.2f}MB")
        
        # Статистика по ключам
        print(f"Всего ключей в базе: {r.dbsize()}")

if __name__ == "__main__":
    # Заполняем до 90MB (оставляем 10MB для операций)
    fill_memory(90)
