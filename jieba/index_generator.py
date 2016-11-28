import jieba
import pymysql


class IndexGenerator:
    def __init__(self):
        self.db = pymysql.connect(host='localhost',
                                  user='root',
                                  passwd='jiazequn',
                                  db='spider',
                                  port=3306,
                                  charset='utf8')
        self.cursor = self.db.cursor()
        self.cursor_insert = self.db.cursor()

    def generate(self):
        self.cursor.execute("select * from pages where id >= 8557")
        result = self.cursor.fetchall()
        for item in result:
            print("processing #", item[0])
            content = item[3].replace("\r", "").replace(" ", "")
            seg_list = jieba.cut_for_search(content)
            sql = "insert ignore into mapping(term_id, page_id) values "
            params = list()
            hasMapping = False
            for term in seg_list:
                term_id = self.get_or_create_term(term)
                params.append(int(term_id))
                params.append(int(item[0]))
                sql += "(%s, %s), "
                hasMapping = True
            sql = sql.strip(", ")
            if hasMapping:
                cursor = self.db.cursor()
                cursor.execute(sql, params)
            self.db.commit()

    def close(self):
        self.db.commit()
        self.db.close()

    def get_or_create_term(self, term):
        cursor = self.db.cursor()
        cursor.execute("select * from dict where term=%s", term)
        if cursor.rowcount >= 1:
            result = cursor.fetchone()
            return result[0]
        else:
            cursor.execute("insert into dict (term) values (%s)", term)
            self.db.commit()
            return cursor.lastrowid

if __name__ == "__main__":
    jieba.enable_parallel(8)
    generator = IndexGenerator()
    generator.generate()
