import db_operations


def fetch_idp_register(conn, partner_id, subject_id):
    """
    Busca si existe un registro de IDP para un estudiante en una asignatura.
    """
    query_select = """
        SELECT estudiante_id, asignatura_id
        FROM repositorio_estudiante_indice_participacion
        WHERE estudiante_id = %s AND asignatura_id = %s
        LIMIT 1
    """
    return db_operations.execute_query(conn, query_select, params=(partner_id, subject_id), fetch=True)


def fetch_enrollments(conn, subject_id):
    """
    Obtiene las matriculaciones de estudiantes en una asignatura específica. Cuyo estado de matriculacion es CONFIRMADO
    o EGRESADO.
    """
    query = """
        SELECT id, partner_id, asignatura_id, estado, fecha, fecha_egreso, ultimo_ingreso
        FROM repo_aden_enrollment
        WHERE asignatura_id = %s
          AND (estado = 'confirmado' OR estado = 'egresado')
          AND programa_id IS NULL
          AND objeto_aprendizaje_id IS NULL;
    """
    return db_operations.execute_query(conn, query, params=(subject_id,), fetch=True)


def update_idp_register(conn, scores, idp_student, partner_id, subject_id, performance):
    """
    Actualiza el registro de IDP para un estudiante existente en una asignatura.
    """
    query_update = """
        UPDATE repositorio_estudiante_indice_participacion
        SET visualizacion_recursos = %s, 
            actividades_presentadas = %s, 
            velocidad_cursado = %s, 
            dias_inactividad = %s, 
            indice_participacion = %s, 
            rendimiento = %s
        WHERE estudiante_id = %s AND asignatura_id = %s;
    """
    db_operations.execute_query(conn, query_update, params=(
        *scores, idp_student, performance, partner_id, subject_id), fetch=False)


def insert_idp_register(conn, partner_id, subject_id, scores, idp_student, enrollment_id, performance):
    """
    Inserta un nuevo registro de IDP para un estudiante en una asignatura.
    """
    query_insert = """
        INSERT INTO repositorio_estudiante_indice_participacion (
            estudiante_id,
            asignatura_id,
            visualizacion_recursos,
            actividades_presentadas,
            velocidad_cursado,
            dias_inactividad,
            indice_participacion,
            enrollment_id,
            rendimiento
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    db_operations.execute_query(conn, query_insert, params=(
        partner_id, subject_id, *scores, idp_student, enrollment_id, performance
    ), fetch=False)
