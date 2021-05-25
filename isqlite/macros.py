def foreign_key_column(name, other_table, *, required):
    not_null = "NOT NULL" if required else ""
    return f"{name} INTEGER {not_null} REFERENCES {other_table}"


def text_column(name, *, required):
    constraint = f"CHECK({name} != '')" if required else ""
    return f"{name} TEXT NOT NULL {constraint}"
