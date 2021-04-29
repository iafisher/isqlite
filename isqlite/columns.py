class BaseColumn:
    def __init__(self, name, *, extra="", required=False):
        self.name = name
        self.extra = extra
        self.required = required


class Text(BaseColumn):
    def __str__(self):
        builder = [self.name, "TEXT", "NOT NULL"]
        if self.required:
            builder.append(f"CHECK({self.name} != '')")
        builder.append(self.extra)
        return " ".join(builder)


class Integer(BaseColumn):
    def __init__(self, *args, autoincrement=False, primary_key=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.autoincrement = autoincrement
        self.primary_key = primary_key

    def __str__(self):
        builder = [self.name, "INTEGER"]
        if self.required:
            builder.append("NOT NULL")
        if self.primary_key:
            builder.append("PRIMARY KEY")
        if self.autoincrement:
            builder.append("AUTOINCREMENT")
        builder.append(self.extra)
        return " ".join(builder)


class Real(BaseColumn):
    def __str__(self):
        builder = [self.name, "REAL"]
        if self.required:
            builder.append("NOT NULL")
        builder.append(self.extra)
        return " ".join(builder)


class Blob(BaseColumn):
    def __str__(self):
        builder = [self.name, "BLOB"]
        if self.required:
            builder.append("NOT NULL")
        builder.append(self.extra)
        return " ".join(builder)


class Timestamp(BaseColumn):
    def __str__(self):
        builder = [self.name, "TIMESTAMP"]
        if self.required:
            builder.append("NOT NULL")
        builder.append(self.extra)
        return " ".join(builder)


class Boolean(BaseColumn):
    def __str__(self):
        builder = [self.name, "BOOLEAN"]
        if self.required:
            builder.append("NOT NULL")
        builder.append(self.extra)
        return " ".join(builder)


class Decimal(BaseColumn):
    def __str__(self):
        builder = [self.name, "DECIMAL"]
        if self.required:
            builder.append("NOT NULL")
        builder.append(self.extra)
        return " ".join(builder)


class Time(BaseColumn):
    def __str__(self):
        builder = [self.name, "TIME"]
        if self.required:
            builder.append("NOT NULL")
        builder.append(self.extra)
        return " ".join(builder)


class ForeignKey(BaseColumn):
    def __init__(self, name, other_table, **kwargs):
        super().__init__(name, **kwargs)
        self.other_table = other_table

    def __str__(self):
        builder = [self.name, "INTEGER"]
        if self.required:
            builder.append("NOT NULL")
        builder.append("REFERENCES")
        builder.append(self.other_table)
        builder.append(self.extra)
        return " ".join(builder)
