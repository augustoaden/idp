import logging

import db_operations
from idp import idp_calculator

# Configuración del logger
logging.basicConfig(
    filename='idp_calculation.log',
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def main():
    try:
        # Connect to the database
        conn = db_operations.connect_db()
        logging.info("Database connection established successfully.")

        # Execute the IDP calculation function
        idp_calculator.calculate_idp_subjects(conn)

        # If everything executes correctly
        logging.info("IDP calculation execution completed successfully.")

    except Exception as e:
        # Log any unexpected errors during the main script execution
        logging.error(f"Error during main execution: {str(e)}", exc_info=True)

    finally:
        # Safely close the connection
        try:
            if conn:
                conn.close()
                logging.info("Database connection closed successfully.")
        except Exception as e:
            logging.error(f"Error closing the database connection: {str(e)}", exc_info=True)


if __name__ == "__main__":
    main()
