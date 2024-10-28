import logging
import datetime

import db_operations
from idp import idp_database_helpers
from idp import idp_student_performance


def calculate_idp_student_in_subject(conn, partner_id, subject_id, enrollment_id, weights_idp_subject, dates_enrollment):
    """
    Calcula el IDP del estudiante en una asignatura.

    :param conn: Conexión a la base de datos.
    :param partner_id: ID del estudiante.
    :param subject_id: ID de la asignatura.
    :param enrollment_id: ID de la matrícula.
    :param weights_idp_subject: Ponderaciones para el cálculo del IDP. (visualización de recursos, actividades presentadas,
    velocidad de cursado, días de inactividad)
    :param dates_enrollment: Fechas de matrícula del estudiante. (fecha de inicio, fecha de egreso, último ingreso)
    :return float: IDP del estudiante en la asignatura.
    """
    try:
        # Obtener los puntajes IDP del estudiante
        scores = idp_student_performance.get_idp_scores(conn, partner_id, subject_id, dates_enrollment)

        # Calcular el IDP del estudiante
        idp_student = idp_student_performance.calculate_idp(scores, weights_idp_subject)

        # Evaluar el rendimiento del estudiante
        performance = idp_student_performance.student_performance_in_subject(idp_student)

        # Verificar si el estudiante ya tiene un registro de IDP en la asignatura
        idp_register = idp_database_helpers.fetch_idp_register(conn, partner_id, subject_id)

        # Actualizar o insertar el registro de IDP del estudiante
        if idp_register:
            idp_database_helpers.update_idp_register(conn, scores, idp_student, partner_id, subject_id, performance)
        else:
            idp_database_helpers.insert_idp_register(conn, partner_id, subject_id, scores, idp_student, enrollment_id, performance)

    except Exception as e:
        logging.error(f"Error calculating IDP for student {partner_id} in subject {subject_id}: {str(e)}", exc_info=True)


def calculate_idp_subject(conn, subject_id, weights_idp_subject):
    """
    Calcula el IDP de estudiantes en una asignatura de la que previamente se ha obtenido sus ponderaciones.

    Para el caso de alumnos egresados se omite el cálculo si su fecha de egreso es distinta a la actual.

    :param conn: Conexión a la base de datos.
    :param subject_id: ID de la asignatura.
    :param weights_idp_subject: Ponderaciones para el cálculo del IDP. (visualización de recursos, actividades presentadas,
    velocidad de cursado, días de inactividad)
    """
    try:
        # Obtener los estudiantes matriculados a la asignatura
        students_enrolled = idp_database_helpers.fetch_enrollments(conn, subject_id)
        students_top_ten = []

        for students_enrolled in students_enrolled:
            try:
                # Si el alumno está egresado y su fecha de egreso es distinta a la actual entonces se omite
                if (students_enrolled[3] == 'egresado') and (students_enrolled[5] != datetime.datetime.now().date()):
                    continue

                calculate_idp_student_in_subject(
                    conn,
                    partner_id=students_enrolled[1],
                    subject_id=subject_id,
                    enrollment_id=students_enrolled[0],
                    weights_idp_subject=weights_idp_subject,
                    dates_enrollment=(students_enrolled[4], students_enrolled[5], students_enrolled[6])
                )

                students_top_ten.append(students_enrolled[0])

            except Exception as e:
                logging.error(f"Error processing student {students_enrolled[1]} in subject {subject_id}: {str(e)}", exc_info=True)


        # Calcular el 10% de los estudiantes con mejor rendimiento en la asignatura
        try:
            idp_student_performance.top_ten(conn, subject_id, students_top_ten)
        except Exception as e:
            logging.error(f"Error calculating top ten students for subject {subject_id}: {str(e)}", exc_info=True)

    except Exception as e:
        logging.error(f"Error calculating IDP for subject {subject_id}: {str(e)}", exc_info=True)


def calculate_idp_subjects(conn):
    """
    Calcula el IDP de las asignaturas que tienen definidas las ponderaciones.

    :param conn: Conexión a la base de datos.
    """
    try:
        query = """
            SELECT asignatura_id, visualizacion_recursos_ponderacion, actividades_presentadas_ponderacion, 
                   velocidad_cursado_ponderacion, dias_inactividad_ponderacion
            FROM repositorio_estudiante_indice_participacion_ponderacion
            WHERE asignatura_id = 1860
        """
        idp_weightings = db_operations.execute_query(conn, query, fetch=True)

        for register in idp_weightings:
            try:
                calculate_idp_subject(
                    conn,
                    subject_id=register[0],
                    weights_idp_subject=(register[1], register[2], register[3], register[4])
                )
            except Exception as e:
                logging.error(f"Error processing subject {register[0]}: {str(e)}", exc_info=True)

    except Exception as e:
        logging.error(f"Error fetching IDP weightings: {str(e)}", exc_info=True)
