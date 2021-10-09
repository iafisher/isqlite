from isqlite import Table, TextColumn

SCHEMA = [
    Table(
        "books",
        columns=[
            TextColumn("title"),
            TextColumn("author"),
        ],
    )
]
