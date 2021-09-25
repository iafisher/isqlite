from isqlite import (
    BooleanColumn,
    DecimalColumn,
    ForeignKeyColumn,
    IntegerColumn,
    Table,
    TextColumn,
)


class Departments(Table):
    name = TextColumn(required=True)
    abbreviation = TextColumn(required=True)


class Professors(Table):
    first_name = TextColumn(required=True)
    last_name = TextColumn(required=True)
    department = ForeignKeyColumn(model="departments", required=True)
    tenured = BooleanColumn(required=True)
    retired = BooleanColumn(required=True)
    manager = ForeignKeyColumn(model="professors")


class Courses(Table):
    course_number = IntegerColumn(required=True)
    department = ForeignKeyColumn(model="departments", required=True)
    instructor = ForeignKeyColumn(model="professors", required=True)
    title = TextColumn(required=True)
    credits = DecimalColumn(required=True)


class Students(Table):
    student_id = IntegerColumn(required=True)
    first_name = TextColumn(required=True)
    last_name = TextColumn(required=True)
    major = ForeignKeyColumn(model="departments")
    graduation_year = IntegerColumn(required=True)
