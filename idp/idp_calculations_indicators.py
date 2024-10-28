import datetime
import db_operations


def get_activities_to_be_presented(conn, subject_id):
    query = """
        SELECT racas.asignatura_id,
        count(*) AS trabajos_a_presentar
        FROM repo_aden_calificacion_asignatura racas
        WHERE racas.asignatura_id = %s
        GROUP BY racas.asignatura_id;
    """
    activities_to_be_presented = db_operations.execute_query(conn, query, params=(subject_id,), fetch=True)
    return activities_to_be_presented[0][1] if activities_to_be_presented else 0


def get_activities_presented(conn, subject_id, partner_id):
    query = """
        SELECT raca.partner_id,
        raca.asignatura_id,
        count(raca.puntaje_obtenido) AS cant_notas
        FROM repo_aden_calificacion_alumno raca
        WHERE raca.asignatura_id = %s AND raca.partner_id = %s
        GROUP BY raca.partner_id, raca.asignatura_id;
    """
    activities_presented = db_operations.execute_query(conn, query, params=(subject_id, partner_id), fetch=True)
    return activities_presented[0][2] if activities_presented else 0


def visualization_resources(conn, partner_id, subject_id):
    """
    Se obtiene la visualización de los recursos en función del progreso total del alumno en la asignatura.

    @:param partner_id: ID del estudiante.
    @:param subject_id: ID de la asignatura.
    @:return float: Visualización de recursos o mensaje de error.
    """
    query = """
    SELECT progreso 
    FROM repo_aden_progreso_total 
    WHERE partner_id = %s AND asignatura_id = %s AND oa_id is null
    """
    total_progress = db_operations.execute_query(conn, query, params=(partner_id, subject_id), fetch=True)

    if total_progress:
        return round(total_progress[0][0] / 100 if total_progress[0][0] else 0, 2)
    else:
        return 0.0


def submission_activities(conn, partner_id, subject_id):
    """
    Se obtiene el porcentaje de entregas realizadas por el alumno en relación a las entregas que debe realizar de las
    actividades asociadas a la asignatura.

    :param partner_id: ID del estudiante.
    :param subject_id: ID de la asignatura.
    :return float: Entrega de actividades.
    """
    activities_to_be_presented = get_activities_to_be_presented(conn, subject_id)
    activities_presented = get_activities_presented(conn, subject_id, partner_id)

    if activities_to_be_presented == 0:
        return 0.0

    return round(activities_presented / activities_to_be_presented, 2)


def set_student_ideal_date_in_subject(duration_subject, duration_time_unit_subject, enrollment_start_date):
    """
    Calcula la fecha ideal del cursado de un estudiante en una asignatura en función de la duración de la asignatura y la
    fecha de inicio de la matriculación.
    """
    if not (duration_subject and duration_time_unit_subject) or not enrollment_start_date:
        return False

    if duration_time_unit_subject == 'dias':
        return enrollment_start_date + datetime.timedelta(days=duration_subject)
    elif duration_time_unit_subject == 'semanas':
        return enrollment_start_date + datetime.timedelta(weeks=duration_subject)
    elif duration_time_unit_subject == 'meses':
        return enrollment_start_date + datetime.timedelta(weeks=duration_subject * 4)


def calculate_velocity_coursed_graduate_student(enrollment_graduate_date, ideal_course_data):
    """
    Calcula la velocidad de cursado de un alumno egresado de una asignatura.
    :param enrollment_graduate_date:
    :param ideal_course_data:
    :return:
    """
    days_graduate = (enrollment_graduate_date - ideal_course_data).days

    if days_graduate <= 0:
        return 1
    elif 1 <= days_graduate <= 7:
        return 3 / 4
    elif 8 <= days_graduate <= 14:
        return 2 / 4
    else:
        return 1 / 4


def calculate_velocity_coursed_confirmed_student(ideal_course_data):
    """
    Calcula la velocidad de cursado de un alumno activo en una asignatura.
    :param ideal_course_data: Fecha ideal de cursado.
    :return: Velocidad de cursado.
    """
    days_delay = (datetime.datetime.now().date() - ideal_course_data).days

    if days_delay < 0:
        return 1
    elif days_delay == 0:
        return 3 / 4
    elif 1 <= days_delay <= 7:
        return 2 / 4
    else:
        return 1 / 4


def velocity_coursed(conn, subject_id, enrollment_start_date, enrollment_graduate_date):
    """
    Se determina la velocidad de cursado en función de la diferencia entre la fecha de egreso y la fecha ideal (caso de
    alumno egresado) o la fecha actual y la fecha ideal (caso de alumno activo).

    La fecha ideal se determina sumando la duración de la asignatura a la fecha de inicio de la matrículación.

    En caso de que la asignatura no tenga asignada una duración y su correspondiente unidad o que no se conozca la fecha
    de inicio de la matriculación, se retorna el valor más bajo: 0.

    :param subject_id: ID de la asignatura.
    :param enrollment_start_date: Fecha de inicio de la matriculación.
    :param enrollment_graduate_date: Fecha de egreso del alumno.
    :return float: Velocidad de cursado.
    """
    query = """
        SELECT ra.duracion, ra.duracion_unidad_tiempo
        FROM repo_aden_asignatura ra
        WHERE ra.id = %s;
    """
    subject_dates = db_operations.execute_query(conn, query, params=(subject_id,), fetch=True)
    valuation = 0.0

    if not subject_dates:
        return valuation

    # Determinacion de la fecha ideal
    ideal_data = set_student_ideal_date_in_subject(subject_dates[0][0], subject_dates[0][1], enrollment_start_date)
    if not ideal_data:
        return valuation

    # Caso alumno EGRESADO
    if enrollment_graduate_date:
        return calculate_velocity_coursed_graduate_student(enrollment_graduate_date, ideal_data)

    # Caso alumno CONFIRMADO
    else:
        return calculate_velocity_coursed_confirmed_student(ideal_data)


def amount_inactive_days(enrollment_last_login):
    """
    Se obtiene la cantidad de días inactivos de un alumno en una asignatura a partir de la diferencia entre la fecha
    actual y el ultimo ingreso del alumno en la asignatura, cuyo dato se encuentra en la tabla "enrollment".

    En caso de que no se conozca la fecha de último ingreso, se retorna la valoración más baja: 1/3.

    @:param enrollment_last_login: Fecha del último ingreso del alumno en la asignatura.
    @:return float: Días de inactividad.
    """
    days_inactive = 1/3

    if enrollment_last_login:
        days_inactive = (datetime.datetime.now().date() - enrollment_last_login.date()).days
        if days_inactive < 7:
            days_inactive = 1
        elif days_inactive < 14:
            days_inactive = 2/3

    return round(days_inactive, 2)
