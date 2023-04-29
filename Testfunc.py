import pandas as pd
import numpy
import sqlite3
import zipfile
import os
from shutil import rmtree, move
from PIL import Image


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
    
    def get_row_count(self):
        self.c = self.conn.cursor()
        self.c.execute("SELECT COUNT(img1) FROM img")
        row = self.c.fetchone()
        self.c.close()
        return row[0]

def extract_exel_img(excel_file = 'expForex.xlsx', folder_save_img = 'images'):
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
def count_files(directory):
    count = 0
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            count += 1
    return count

def img_to_df(data_df=pd.DataFrame(), img_path = 'images'):
    data = data_df.copy()
    count_img = count_files(img_path)
    try:
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

df = pd.read_excel('expForex.xlsx')   
extract_exel_img('expForex.xlsx','images')
df = img_to_df(df)

db = DBImg("FXEXP.DB")
print(db.get_row_count())
df_get = db.get_one_df(12)

db.insert_df(df)