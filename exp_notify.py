#!/usr/bin/env python3
from pytz import timezone
import requests
import datetime
import time
import sqlite3
from telegram.ext import Updater, MessageHandler, Filters, CommandHandler
import os 
import threading
import random
import zipfile
from shutil import move, rmtree
import pandas as pd

xlsx_name = 'expForex.xlsx'

db_name = 'FXEXP.DB'
wait_time = 3600
event_changexlsx = threading.Event()

# Class DB img
class DBImg:
    def __init__(self, db_name):
        self.db_name = db_name
        self.conn = sqlite3.connect(self.db_name)
        self.c = self.conn.cursor()
        self.c.execute('''CREATE TABLE IF NOT EXISTS img (
          id INTEGER PRIMARY KEY,
          pair TEXT,
          order_type TEXT,
          time TEXT,
          type TEXT,
          condition TEXT,
          alert TEXT,
          tp int,
          note TEXT, 
          img1 BLOB, img2 BLOB )''')
        self.conn.commit()
        self.c.close()

    def insert_df(self, df_data):
        self.c = self.conn.cursor()
        for index, row in df_data.iterrows():
            # Check nan image
            if str(row['Image1']).lower() ==  'nan': 
                continue

            # add to execute
            self.c.execute("INSERT OR IGNORE INTO img (id, pair, order_type, time, type, condition, alert, tp, note, img1, img2) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (row['ID'], row['Pair'], row['Order type'], row['Time'], row['Type'], row['Condition'], row['Alert'], row['TP'], row['Note'], row['Image1'], row['Image2']))
        self.conn.commit()
        self.c.close()

    def get_df(self):
        self.c = self.conn.cursor()
        self.c.execute("SELECT * FROM img")
        rows = self.c.fetchall()
        self.c.close()
        return pd.DataFrame(rows, columns=['ID', 'Pair', 'Order type', 'Time', 'Type', 'Condition', 'Alert', 'TP', 'Note', 'Image1', 'Image2'])

    def get_one_df(self, id):
        self.c = self.conn.cursor()
        self.c.execute("SELECT * FROM img WHERE id=?", (id,))
        row = self.c.fetchone()
        self.c.close()
        return row
    
    def get_rowimg_count(self):
        self.c = self.conn.cursor()
        self.c.execute("SELECT COUNT(img1) FROM img")
        row = self.c.fetchone()
        self.c.close()
        return row[0]
    
    def close(self):
        self.conn.close()

def extract_exel_img(excel_file = 'expForex.xlsx', folder_save_img = 'images'):
  try:
     # Mở file zip
    with zipfile.ZipFile(excel_file, "r") as zip_ref:
      # Giải nén tất cả các tệp trong thư mục xl/media ra một thư mục tạm thời
      zip_ref.extractall(path="tmp", members=[f for f in zip_ref.namelist() if f.startswith('xl/media/')])

    # Xóa folder images nếu đã tồn tại
    if os.path.exists("images"):
      rmtree('images')
      os.makedirs("images")
    else:
      os.makedirs("images")

    #remove all file from tmp/xl/media to folder_save_img
    for filename in os.listdir('tmp/xl/media'):
      move('tmp/xl/media/' + filename, folder_save_img)

    # Xóa thư mục tạm thời
    rmtree("tmp")
    return True
  except Exception as e:
    print("Error extract img from excel: " + str(e))
    return False
def img_to_df(data_df=pd.DataFrame(), img_path = 'images'):
  # Hàm thêm ảnh vào DB và xóa toàn bộ hàng không có image
  try:
    data = data_df.copy()
    count_img = count_files(img_path)
    for idx, row in data.iterrows():
        img_id = row['ID']
        if count_img < img_id * 2:
            break
        with open(f'images/image{img_id*2-1}.png', 'rb') as f:
            img = f.read()
        data.at[idx, 'Image1'] = img
        with open(f'images/image{img_id*2}.png', 'rb') as f:
            img = f.read()
        data.at[idx, 'Image2'] = img
    # Xóa tất cả các hàng không có image
    data = data.dropna(subset=['Image1'])
    return data
  except Exception as e:
      print("Error add image to dataframe: "+ str(e))
    

#open file and write log
def write_log(text, filename='log_forex.txt'):
    #get time format
    now = datetime.datetime.now(tz=timezone('Asia/Ho_Chi_Minh'))
    #now = now + datetime.timedelta(hours=7)
    time = now.strftime("%H:%M:%S")
    text = time + ' - ' + text
    with open('log/log.txt', 'a', encoding='utf-8') as f:
        f.write(text + '\n')

def save_img_from_word(filename, folder_save_img = 'images'):
  # Delete folder if exists
  from shutil import rmtree
  if os.path.exists(folder_save_img):
    rmtree(folder_save_img)
  
  from docx2txt import process
  # Create folder if not exists
  if not os.path.exists(folder_save_img):
    os.makedirs(folder_save_img)

  # Save images from word file
  
  text = process(filename, folder_save_img)

  return

def count_files(directory):
    count = 0
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            count += 1
    return count

def photo_handler(update, context):
    # Lấy hình ảnh từ tin nhắn của người dùng
    photo = update.message.photo[-1]

    # Lấy thông tin của tệp tin ảnh
    file = context.bot.get_file(photo.file_id)

    # Lấy đường dẫn đến tệp tin ảnh
    file_path = file.file_path
    
    # Lấy dữ liệu của hình ảnh từ đường dẫn tệp tin ảnh
    image_data = requests.get(file_path).content

    update.message.reply_text('Đã nhận được ảnh!')

def schedule_notify(bot):
  # Gửi tin nhắn đến 1 người dùng
  chat_id = 1042979764 
  id_img_old = 0
  db = DBImg(db_name)

  while 1:
    try:
      # Get count in DB
      db_count = db.get_rowimg_count()
      if db_count == 0:
        bot.send_message(chat_id=chat_id, text="Không có dữ liệu trong DB!")
        event_changexlsx.wait(wait_time)
        event_changexlsx.clear()
        continue

      # Get random id and get row in DB
      while 1:
        rand_id = random.randint(1, db_count)
        if rand_id != id_img_old:
          break
      
      # Get one row in DB
      id_img_old = rand_id
      row = db.get_one_df(rand_id)

      # parse text send
      lst_head = ['ID', 'Pair', 'Order Type', 'Time', 'Type', 'Condition', 'Alert', 'TP', 'Note']
      img_send = []
      text_send = ""
      for i in range(0, len(row)):
        if type(row[i]) == bytes:
          # Image parse
          img_send.append(row[i])
        else:
          # Test parse
          text_send = text_send + lst_head[i] + ": " + str(row[i]) + "\n"

      # replace \n\n to \n
      text_send = text_send.replace('\n\n', '\n')

      # bot.send_message(chat_id=chat_id, text=str_time)
      bot.send_message(chat_id=chat_id, text=text_send)
      bot.send_photo(chat_id=chat_id, photo=img_send[0])
      bot.send_photo(chat_id=chat_id, photo=img_send[1])

      # Time sleep
      print(f"Sleeping {wait_time} seconds...")
      
      # Sleep 1h
      time.sleep(wait_time)

    except Exception as e:
      print("Exception: ", e)
      bot.send_message(chat_id=chat_id, text="Exception: " + str(e))
      time.sleep(wait_time)

def start_handle(update, context):
  update.message.reply_text('Xin chào!')
  #chat_id = update.message.chat_id
  #print("chat_id_notify: ", chat_id)
  #context.job_queue.run_once(schedule_notify, 1, context=chat_id)

def update_db_xlsx(update, context):
  try:
    # check if file is document
    if not update.message.document:
      update.message.reply_text('Gửi file .xlsx để update !!!')
      return
    
    # Lấy file về
    file = context.bot.get_file(update.message.document.file_id)
    filepath = file.file_path

    # Kiểm tra xem tên file có kết thúc bằng '.xlsx' hay không
    if filepath.endswith('.xlsx'):
      # Tải file xuống và lưu nó
      update.message.reply_text('Đợi tý đang tải file...')
      response = requests.get(filepath)
      with open(xlsx_name, "wb") as f:
          f.write(response.content)
      
      # Lưu file và update db
      df = pd.read_excel(xlsx_name)   
      extract_exel_img(xlsx_name,'images')
      df = img_to_df(df, 'images')

      # update DB
      db = DBImg(db_name)
      db.insert_df(df)
      db.close()
      update.message.reply_text('Đã add db!')
    else:
        update.message.reply_text("File không hợp lệ. Vui lòng gửi file định dạng .xlsx")
  except Exception as e:
    update.message.reply_text("Exception update db: " + str(e))

def pair_handle(update, context):
  pass
def timesend_handle(update, context):
  update.message.reply_text('Cú pháp: timesend <seconds>')
  pass
def type_handle(update, context):
  pass
def other_text_handle(update, context):
  text = update.message.text
  if text.startswith('timesend'):
    # update time send
    global wait_time
    try:
      time_send = int(text.split(' ')[1])
      if time_send > 0:
        wait_time = time_send
        update.message.reply_text('Đã update time send = ' + str(wait_time))
      else:
        update.message.reply_text('Time send phải lớn hơn 0!')
    except Exception as e:
      update.message.reply_text('Exception time send: ' + str(e))

def main():
  
  token = "6179912911:AAGMg3od1lNvVQunmpyWRkUOe77mkjNWa1s" # anhdz_fxexp_bot
  updater = Updater(token=token, use_context=True)
  dispatcher = updater.dispatcher

  # Update handle to dispatcher
  dispatcher.add_handler(CommandHandler('start', start_handle))
  dispatcher.add_handler(CommandHandler('xlsx', update_db_xlsx))
  dispatcher.add_handler(CommandHandler('pair', pair_handle))
  dispatcher.add_handler(CommandHandler('time_send', timesend_handle))
  dispatcher.add_handler(CommandHandler('type', type_handle))

  # handle message to image
  dispatcher.add_handler(MessageHandler(Filters.document, update_db_xlsx))
  #dispatcher.add_handler(MessageHandler(Filters.photo, photo_handler))

  # any text
  dispatcher.add_handler(MessageHandler(Filters.text, other_text_handle))

  updater.start_polling()
  print("Bot is running...")

  # thread send notify
  thread = threading.Thread(target=schedule_notify, args=(updater.bot,))
  thread.daemon = True
  thread.start()

  updater.idle()
# Main
if __name__ == "__main__":
  main()

