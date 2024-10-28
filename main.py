import db_operations
from idp import idp_calculator


def main():
    conn = db_operations.connect_db()
    idp_calculator.calculate_idp_subjects(conn)
    conn.close()


if __name__ == "__main__":
    main()
