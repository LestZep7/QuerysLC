WITH
    afiliados_con_solicitud_vejez AS (
        SELECT DISTINCT
            sc_inner.NUMERO_AFILIADO
        FROM
            SIHIS.SIHIS_CASO          sc_inner
            JOIN IVS.IVS_REQUERIMIENTOS ir_inner ON sc_inner.COD_REQUERIMIENTO_SIIVS = ir_inner.COD_REQUERIMIENTO
            JOIN IVS.IVS_SOLICITUDES    ivs_inner ON ir_inner.COD_SOLICITUD = ivs_inner.COD_SOLICITUD
        WHERE
            ivs_inner.COD_TIPO_SOLICITUD = 1
            AND sc_inner.ID_RIESGO = 'V'
    ),
    traslados_agrupados AS (
        SELECT
            COD_CASO,
            MIN(CASE WHEN COD_ETAPA_DESTINO = 2 THEN FECHA_ING END) AS FECHA_RECEPCION,
            MIN(CASE WHEN COD_ETAPA_DESTINO = 13 THEN FECHA_ING END) AS FECHA_ARCHIVO,
            MIN(CASE WHEN COD_ETAPA_ORIGEN = 13 AND COD_ETAPA_DESTINO = 3 THEN FECHA_ING END) AS FECHA_TRASLADO_INVESTIGADOR,
            MIN(CASE WHEN COD_ETAPA_ORIGEN = 3 AND COD_ETAPA_DESTINO = 10 THEN FECHA_ING END) AS FECHA_ARCHIVO_REQUERIMIENTOS,
            MIN(CASE WHEN COD_ETAPA_ORIGEN = 3 AND COD_ETAPA_DESTINO = 5 THEN FECHA_ING END) AS FECHA_TRASLADO_REVISOR,
            MIN(CASE WHEN COD_ETAPA_ORIGEN = 5 AND COD_ETAPA_DESTINO = 6 THEN FECHA_ING END) AS FECHA_TRASLADO_SUPERVISOR,
            MAX(CASE WHEN COD_ETAPA_DESTINO = 8 THEN FECHA_ING END) AS FECHA_DESPACHO
        FROM
            SIHIS.SIHIS_TRASLADO
        GROUP BY
            COD_CASO
    ),
    ultima_etapa_cte AS (
        SELECT
            sh.COD_CASO,
            MAX(sh.COD_TRASLADO) KEEP(
                DENSE_RANK LAST ORDER BY
                    sh.FECHA_ING
            ) AS ULTIMO_TRASLADO_ID
        FROM
            SIHIS.SIHIS_TRASLADO sh
        WHERE
            sh.ESTADO != 0
        GROUP BY
            sh.COD_CASO
    ),
    ingresos_por_caso AS (
        SELECT
            COD_CASO,
            COUNT(1) AS INGRESOS
        FROM
            SIHIS.SIHIS_INGRESO
        WHERE
            ESTADO = 1
        GROUP BY
            COD_CASO
    )
SELECT
    sc.COD_CASO                                                                                           AS "CÓDIGO DE CASO",
    sc.NUMERO_AFILIADO                                                                                    AS "NÚMERO DE AFILIADO",
    CASE
        WHEN ra.actualizado_nuevproc = 'N' THEN TO_CHAR(ra.noma)
        ELSE TRIM(ra.primer_nombre || ' ' || ra.segundo_nombre) || ', ' || TRIM(ra.primer_apellido || ' ' || ra.segundo_apellido)
    END                                                                                                   AS "NOMBRE AFILIADO",
    ra.FECHA_NACIMIENTO                                                                                   AS "FECHA NACIMIENTO",
    CASE
        WHEN sc.ID_RIESGO = 'S' THEN NULL
        ELSE FLOOR(MONTHS_BETWEEN(SYSDATE, ra.FECHA_NACIMIENTO) / 12)
    END                                                                                                   AS "EDAD ACTUAL",
    CASE
        WHEN sc.ID_RIESGO = 'S' THEN 'NO APLICA'
        ELSE
            CASE
                WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, ra.FECHA_NACIMIENTO) / 12) >= 60 THEN 'YA CUMPLIÓ 60'
                WHEN FLOOR(MONTHS_BETWEEN(SYSDATE, ra.FECHA_NACIMIENTO) / 12) = 59 THEN 'PRÓXIMO A CUMPLIR 60'
                ELSE 'NO APLICA'
            END
    END                                                                                                   AS "ESTADO EDAD",
    CASE WHEN sc.ID_RIESGO = 'S' THEN NULL ELSE ra.FECHA_FALLECIMIENTO END                                 AS "FECHA FALLECIMIENTO",
    CASE
        WHEN sc.ID_RIESGO = 'S' THEN NULL
        WHEN ra.FECHA_FALLECIMIENTO IS NOT NULL THEN 'YA FALLECIÓ'
        ELSE NULL
    END                                                                                                   AS "ESTADO VITAL",
    DECODE(sc.ID_RIESGO, 'V', 'VEJEZ', 'I', 'INVALIDEZ', 'C', 'CONTRIBUCIÓN VOLUNTARIA', 'S', 'SOBREVIVENCIA', 'N/A') AS RIESGO,
    CASE
        WHEN sc.COD_FUENTE = 3 THEN 'SOLICITUD MANUAL'
        WHEN sc.COD_FUENTE = 5 THEN 'SOLICITUD DE INTERESADO'
        WHEN sc.COD_FUENTE = 6 THEN sct.TIPO_SOLICITUD
        WHEN sc.COD_FUENTE = 7 AND ir.COD_SOLICITUD IS NULL THEN 'SOLICITUD MANUAL'
        WHEN sc.COD_FUENTE = 7 AND ir.COD_SOLICITUD IS NOT NULL THEN its.TIPO_SOLICITUD
    END                                                                                                   AS "TIPO DE SOLICITUD",
    ua.NOMBRE_CORTO_UNIDAD                                                                                AS "UNIDAD EMISORA REQUERIMIENTO",
    DECODE(ua.COD_LOCALIDAD, 1, 'LOCAL', 2, 'DEPARTAMENTAL')                                              AS LOCALIDAD,
    CASE
        WHEN sc.COD_FUENTE IN (3, 5, 6) THEN sd1.DEPENDENCIA
        WHEN sc.COD_FUENTE = 7 THEN sdv.DEPENDENCIA
    END                                                                                                   AS "DEPENDENCIA EMISORA",
    CASE
        WHEN sc.COD_FUENTE = 3 THEN sc.NUMERO_DOCUMENTO
        WHEN sc.COD_FUENTE = 5 THEN sc.COD_REQUERIMIENTO_INT
        WHEN sc.COD_FUENTE = 6 THEN sc.COD_REQUERIMIENTO_SICV
        WHEN sc.COD_FUENTE = 7 THEN sc.COD_REQUERIMIENTO_SIIVS
    END                                                                                                   AS "CÓDIGO REQUERIMIENTO",
    sc.FECHA_DOCUMENTO                                                                                    AS "FECHA DOCUMENTO",
    sc.FECHA_ING                                                                                          AS "FECHA INGRESO",
    CASE
        WHEN sc.COD_FUENTE = 6 THEN suc.PRIMER_NOMBRE || ' ' || suc.PRIMER_APELLIDO
        WHEN sc.COD_FUENTE = 7 THEN su.PRIMER_NOMBRE || ' ' || su.PRIMER_APELLIDO
        ELSE NULL
    END                                                                                                   AS "USUARIO CREÓ REQUERIMIENTO",
    sf.FUENTE,
    DECODE(hpi.COD_TURNO, '1', 'AM', '2', 'PM', '3', 'FDS')                                                AS "TURNO DEL INVESTIGADOR",
    UPPER(hpi.NOMBRE_REPORTE)                                                                             AS "NOMBRE INVESTIGADOR",
    t.FECHA_TRASLADO_INVESTIGADOR                                                                         AS "FECHA TRASLADO INVESTIGADOR",
    UPPER(hpr.NOMBRE_REPORTE)                                                                             AS "NOMBRE REVISOR",
    t.FECHA_TRASLADO_REVISOR                                                                              AS "FECHA TRASLADO REVISOR",
    UPPER(hps.NOMBRE_REPORTE)                                                                             AS "NOMBRE SUPERVISOR",
    t.FECHA_TRASLADO_SUPERVISOR                                                                           AS "FECHA TRASLADO SUPERVISOR",
    t.FECHA_DESPACHO                                                                                      AS "FECHA DESPACHO",
    ec.ESTADO_CASO                                                                                        AS "ESTADO",
    et.ETAPA                                                                                              AS "ETAPA",
    ing.INGRESOS                                                                                          AS "INGRESOS POR CASO",
    hpi.COD_OPERADOR                                                                                      AS "CÓDIGO INVESTIGADOR",
    sc.FECHA_INGRESO                                                                                      AS "FECHA CREACIÓN",
    CASE
        WHEN sc.COD_FUENTE IN (3, 5) THEN NULL
        WHEN sc.COD_FUENTE = 6 THEN scs.FECHA_RECEPCION
        WHEN sc.COD_FUENTE = 7 THEN ivs.FECHA_RECEPCION
    END                                                                                                   AS "FECHA_SOLICITUD",
    CASE
        WHEN sc.COD_FUENTE IN (3, 5) THEN NULL
        WHEN sc.COD_FUENTE = 6 THEN scs.COD_SOLICITUD
        WHEN sc.COD_FUENTE = 7 THEN ivs.COD_SOLICITUD
    END                                                                                                   AS "CÓDIGO SOLICITUD SIIVS-SICV",
    DECODE(ivs.EXISTEN_REPORTE_PREVIOS_IC, NULL, 'NO', '0', 'NO', '1', 'SI', '2', 'NO')                    AS EXISTEN_REPORTE_PREVIOS_IC,
    t.FECHA_ARCHIVO                                                                                       AS "FECHA ARCHIVO",
    t.FECHA_RECEPCION                                                                                     AS "RECHA_RECEPCIÓN",
    t.FECHA_ARCHIVO_REQUERIMIENTOS                                                                        AS "FECHA ARCHIVO REQUERIMIENTOS",
    CASE
        WHEN sc.COD_FUENTE = 5 AND acsv.NUMERO_AFILIADO IS NOT NULL THEN 'TIENE SOLICITUDES POR VEJEZ'
        ELSE NULL
    END                                                                                                   AS "SOLICITUD VEJEZ"
FROM
    SIHIS.SIHIS_CASO sc
    LEFT JOIN afiliados_con_solicitud_vejez   acsv ON sc.NUMERO_AFILIADO = acsv.NUMERO_AFILIADO
    LEFT JOIN traslados_agrupados             t ON sc.COD_CASO = t.COD_CASO
    LEFT JOIN ingresos_por_caso               ing ON sc.COD_CASO = ing.COD_CASO
    LEFT JOIN ultima_etapa_cte                eta ON sc.COD_CASO = eta.COD_CASO
    LEFT JOIN SIHIS.SIHIS_TRASLADO            eta1 ON eta.ULTIMO_TRASLADO_ID = eta1.COD_TRASLADO
    LEFT JOIN SIHIS.SIHIS_ETAPA               et ON eta1.COD_ETAPA_DESTINO = et.COD_ETAPA
    LEFT JOIN SIGSS.RAP_AFILIADOS             ra ON sc.NUMERO_AFILIADO = ra.NUMERO_AFILIADO
    LEFT JOIN SPP.SPP_DEPENDENCIA             sd1 ON sc.COD_DEPENDENCIA = sd1.COD_DEPENDENCIA
    LEFT JOIN SIHIS.SIHIS_FUENTE              sf ON sc.COD_FUENTE = sf.COD_FUENTE
    LEFT JOIN SIHIS.SIHIS_ESTADO_CASO         ec ON sc.ESTADO = ec.COD_ESTADO_CASO
    LEFT JOIN SIHIS.SIHIS_HIST_PUESTO         hpi ON sc.COD_INVESTIGADOR = hpi.SEQ_HIST_PUESTO
    LEFT JOIN SIHIS.SIHIS_HIST_PUESTO         hpr ON sc.COD_REVISOR = hpr.SEQ_HIST_PUESTO
    LEFT JOIN SIHIS.SIHIS_HIST_PUESTO         hps ON sc.COD_SUPERVISOR = hps.SEQ_HIST_PUESTO
    LEFT JOIN IVS.IVS_REQUERIMIENTOS          ir ON sc.COD_REQUERIMIENTO_SIIVS = ir.COD_REQUERIMIENTO
    LEFT JOIN IVS.IVS_SOLICITUDES             ivs ON ir.COD_SOLICITUD = ivs.COD_SOLICITUD
    LEFT JOIN IVS.IVS_TIPO_SOLICITUD          its ON ivs.COD_TIPO_SOLICITUD = its.COD_TIPO_SOLICITUD
    LEFT JOIN SICV.CV_REQUERIMIENTO           scr ON sc.COD_REQUERIMIENTO_SICV = scr.COD_REQUERIMIENTO
    LEFT JOIN SICV.CV_SOLICITUD               scs ON scr.COD_SOLICITUD = scs.COD_SOLICITUD
    LEFT JOIN SICV.CV_TIPO_SOLICITUD          sct ON scs.COD_TIPO_SOLICITUD = sct.COD_TIPO_SOLICITUD
    LEFT JOIN IVS.IVS_DEPENDENCIA             sdv ON ir.COD_DEPENDENCIA_EMISORA = sdv.COD_DEPENDENCIA
    LEFT JOIN SPP.SPP_UNIDAD_ADMINISTRATIVA   ua ON sc.COD_UNIDAD_ADMINISTRATIVA = ua.CODIGO_UNIDAD
    LEFT JOIN SPP.SPP_USUARIO                 su ON ir.USR_SOLICITUD = su.ID_USUARIO
    LEFT JOIN SPP.SPP_USUARIO                 suc ON scr.USR_SOLICITUD = suc.ID_USUARIO
WHERE
    sc.ESTADO != 0