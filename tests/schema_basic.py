from isqlite import Table, TextColumn


class Books(Table):
    title = TextColumn()
    author = TextColumn()
