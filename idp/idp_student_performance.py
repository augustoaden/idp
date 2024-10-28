from idp import idp_calculations_indicators
import db_operations


def get_idp_scores(conn, partner_id, subject_id, dates_enrollment):
    """
    Obtiene los puntajes IDP para un estudiante en una asignatura.
    """
    score_one = idp_calculations_indicators.visualization_resources(conn, partner_id, subject_id)
    score_two = idp_calculations_indicators.submission_activities(conn, partner_id, subject_id)
    score_three = idp_calculations_indicators.velocity_coursed(conn, subject_id, dates_enrollment[0], dates_enrollment[1])
    score_four = idp_calculations_indicators.amount_inactive_days(dates_enrollment[2])

    return [score_one, score_two, score_three, score_four]


def calculate_idp(scores, weights):
    """
    Calcula el IDP basado en los puntajes y las ponderaciones proporcionadas.
    """
    return round((
            scores[0] * weights[0] +
            scores[1] * weights[1] +
            scores[2] * weights[2] +
            scores[3] * weights[3]
    ), 2)


def student_performance_in_subject(idp_student):
    """
    Evaluar el rendimiento del estudiante en función de su IDP.

    Por defecto se retorna 'bajo' si el IDP es menor a 25.

    @:param idp_student: IDP del estudiante.
    @:return str: Rendimiento del estudiante.
    """
    performance = 'bajo'

    if idp_student >= 75:
        performance = 'alto'
    elif 50 <= idp_student < 75:
        performance = 'medio-alto'
    elif 25 <= idp_student < 50:
        performance = 'medio-bajo'

    return performance


def top_ten(conn, subject_id, students_ids):
    """
    Obtiene los 10 mejores estudiantes en función de su IDP.
    """
    # Calcula el número de estudiantes que representa el 10% de los alumnos en la asignatura
    top_ten_count = max(1, int(len(students_ids) * 0.10))

    # Busca estudiantes con rendimiento 'alto' en la asignatura específica
    query_select = """
        SELECT estudiante_id
        FROM repositorio_estudiante_indice_participacion
        WHERE estudiante_id = ANY(%s)
          AND asignatura_id = %s
          AND rendimiento = 'alto'
        ORDER BY indice_participacion DESC
        LIMIT %s
    """

    top_candidates_ids = db_operations.execute_query(conn, query_select, params=(students_ids, subject_id, top_ten_count), fetch=True)

    if top_candidates_ids:
        update_query = """
            UPDATE tu_tabla_estudiantes
            SET top_10 = TRUE
            WHERE estudiante_id IN top_candidates_ids
        """

        db_operations.execute_query(conn, update_query, params=(top_candidates_ids, ), fetch=True)
