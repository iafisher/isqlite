from isqlite import (
    AutoTable,
    BooleanColumn,
    DecimalColumn,
    ForeignKeyColumn,
    IntegerColumn,
    TextColumn,
)


class Departments(AutoTable):
    name = TextColumn(required=True)
    abbreviation = TextColumn(required=True)


class Professors(AutoTable):
    first_name = TextColumn(required=True)
    last_name = TextColumn(required=True)
    department = ForeignKeyColumn(model="departments", required=True)
    # DELETED:
    #   tenured = BooleanColumn(required=True)
    retired = BooleanColumn(required=True)
    manager = ForeignKeyColumn(model="professors")


class Courses(AutoTable):
    course_number = IntegerColumn(required=True)
    department = ForeignKeyColumn(model="departments", required=True)
    instructor = ForeignKeyColumn(model="professors", required=True)
    title = TextColumn(required=True)
    credits = DecimalColumn(required=True)


class Students(AutoTable):
    student_id = IntegerColumn(required=True)
    first_name = TextColumn(required=True)
    last_name = TextColumn(required=True)
    major = ForeignKeyColumn(model="departments")
    graduation_year = IntegerColumn(required=True)
    # ADDED:
    dormitory = TextColumn(required=False)
