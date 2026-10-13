"""
transform.py
Etapa de Transformación del pipeline ETL — Pensión Obligatoria AFP
------------------------------------------------------------------
Aplica las 11 transformaciones definidas en la Entrega 2:
  T1  Eliminación de duplicados exactos en asignación
  T2  Tratamiento de nulos en id_posicion_lider_equipo
  T3  Winsorización de outliers en salario_tope (P99)
  T4  Exclusión del periodo 202606 del análisis de dispersión
  T5  Categorización de ejecutivos sin perfil en jerarquía actual
  T6  Integración de las cuatro fuentes sobre emp_hash64
  T7  Flag de dispersión por empresa entre periodos
  T8  Score de potencial por empresa
  T9  Métricas de desempeño por asesor
  T10 Estandarización de periodos a tipo fecha
  T11 Selección de asignación más reciente por empresa
"""

import numpy as np
import pandas as pd


# ── Helpers ────────────────────────────────────────────────────────────────

def _normalize(series: pd.Series) -> pd.Series:
    mn, mx = series.min(), series.max()
    if mx == mn:
        return series * 0.0
    return (series - mn) / (mx - mn)


# ── Transformaciones individuales ──────────────────────────────────────────

def t1_dedup_asignacion(df: pd.DataFrame) -> pd.DataFrame:
    """T1 — Elimina duplicados exactos por (emp_hash64, periodo_asignacion, id_posicion_ejecutivo)."""
    before = len(df)
    df = df.drop_duplicates(
        subset=["emp_hash64", "periodo_asignacion", "id_posicion_ejecutivo"],
        keep="first"
    )
    print(f"  T1  Duplicados eliminados: {before - len(df):,}")
    return df.reset_index(drop=True)


def t2_nulos_lider(df: pd.DataFrame) -> pd.DataFrame:
    """T2 — Rellena nulos en id_posicion_lider_equipo con 'SIN_LIDER'."""
    mask = df["id_posicion_lider_equipo"].isna() | (df["id_posicion_lider_equipo"].str.strip() == "")
    df.loc[mask, "id_posicion_lider_equipo"] = "SIN_LIDER"
    print(f"  T2  Nulos en líder imputados como SIN_LIDER: {mask.sum():,}")
    return df


def t3_winsorize_salario(df: pd.DataFrame) -> pd.DataFrame:
    """T3 — Winsoriza salario_tope al percentil 99."""
    df["salario_tope"] = pd.to_numeric(df["salario_tope"], errors="coerce")
    p99 = df["salario_tope"].quantile(0.99)
    outliers = (df["salario_tope"] > p99).sum()
    df["salario_tope_win"] = df["salario_tope"].clip(upper=p99)
    print(f"  T3  Salarios winsorizados (> P99={p99:,.0f}): {outliers:,}")
    return df


def t4_excluir_periodo_futuro(df: pd.DataFrame) -> pd.DataFrame:
    """T4 — Excluye el periodo 202606 del análisis de dispersión."""
    df["periodo_asignacion"] = pd.to_numeric(df["periodo_asignacion"], errors="coerce")
    before = len(df)
    df = df[df["periodo_asignacion"] != 202606].copy()
    print(f"  T4  Registros periodo 202606 excluidos: {before - len(df):,}")
    return df.reset_index(drop=True)


def t5_ejecutivos_historicos(df_asig: pd.DataFrame, df_jer: pd.DataFrame) -> pd.DataFrame:
    """T5 — Marca ejecutivos no presentes en jerarquía como EJECUTIVO_HISTORICO."""
    eje_activos = set(df_jer["idEjecutivo"].dropna())
    mask = ~df_asig["id_posicion_ejecutivo"].isin(eje_activos)
    df_asig.loc[mask, "id_tipo_ejecutivo"] = "EJECUTIVO_HISTORICO"
    print(f"  T5  Ejecutivos históricos marcados: {mask.sum():,}")
    return df_asig


def t10_estandarizar_periodos(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """T10 — Convierte columna de periodo YYYYMM a tipo fecha (YYYY-MM-01)."""
    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    df[col + "_fecha"] = pd.to_datetime(
        df[col].astype(str).str[:4] + "-" + df[col].astype(str).str[4:6] + "-01",
        errors="coerce"
    )
    print(f"  T10 Periodo '{col}' convertido a fecha")
    return df


def t11_asignacion_reciente(df: pd.DataFrame) -> pd.DataFrame:
    """T11 — Conserva solo la asignación más reciente por empresa."""
    before = len(df)
    df = (
        df.sort_values("periodo_asignacion", ascending=False)
        .drop_duplicates(subset="emp_hash64", keep="first")
        .reset_index(drop=True)
    )
    print(f"  T11 Filas reducidas de {before:,} a {len(df):,} (asignación más reciente por empresa)")
    return df


def t6_integrar_fuentes(df_asig: pd.DataFrame,
                         df_segm: pd.DataFrame,
                         df_mon_agg: pd.DataFrame) -> pd.DataFrame:
    """T6 — Une asignación vigente + segmentación + agregados de monitoreo."""
    master = df_asig.merge(df_segm, on="emp_hash64", how="left")
    master = master.merge(df_mon_agg, on="emp_hash64", how="left")
    print(f"  T6  Tabla maestra integrada: {len(master):,} empresas")
    return master


def t7_flag_dispersion(df_mon: pd.DataFrame) -> pd.DataFrame:
    """T7 — Calcula flag de dispersión y afiliados perdidos entre periodos."""
    df_mon["periodo"] = pd.to_numeric(df_mon["periodo"], errors="coerce")
    periodos = sorted(df_mon["periodo"].dropna().unique())

    if len(periodos) < 2:
        print("  T7  Solo hay un periodo — flag_dispersion no aplica")
        return pd.DataFrame(columns=["emp_hash64", "flag_dispersion", "afiliados_perdidos"])

    p1, p2 = periodos[0], periodos[-1]
    emps_p1 = set(df_mon[df_mon["periodo"] == p1]["emp_hash64"])
    emps_p2 = set(df_mon[df_mon["periodo"] == p2]["emp_hash64"])

    dispersas = emps_p1 - emps_p2

    # Contar afiliados perdidos
    afil_p1 = df_mon[df_mon["periodo"] == p1].groupby("emp_hash64")["afi_hash64"].count().reset_index()
    afil_p1.columns = ["emp_hash64", "afiliados_p1"]
    afil_p2 = df_mon[df_mon["periodo"] == p2].groupby("emp_hash64")["afi_hash64"].count().reset_index()
    afil_p2.columns = ["emp_hash64", "afiliados_p2"]

    disp_df = afil_p1.merge(afil_p2, on="emp_hash64", how="left")
    disp_df["afiliados_p2"] = disp_df["afiliados_p2"].fillna(0)
    disp_df["afiliados_perdidos"] = disp_df["afiliados_p1"] - disp_df["afiliados_p2"]
    disp_df["flag_dispersion"] = disp_df["emp_hash64"].isin(dispersas).astype(int)

    print(f"  T7  Empresas con dispersión (P1 -> P2): {disp_df['flag_dispersion'].sum():,} "
          f"de {len(emps_p1):,} activas en P1")
    # The dedicated dispersion output contains only positive cases. When this
    # table is joined to the master, missing rows are filled with flag 0.
    return disp_df.loc[
        disp_df["flag_dispersion"].eq(1),
        ["emp_hash64", "flag_dispersion", "afiliados_perdidos", "afiliados_p1", "afiliados_p2"],
    ]


def t8_score_potencial(df: pd.DataFrame) -> pd.DataFrame:
    """T8 — Calcula score de potencial ponderado por empresa."""
    df["score_segmento"] = df["desc_segmento_empleador_actual"].map(
        {"PREMIUM": 1.0, "BASIC": 0.3}
    ).fillna(0.1)
    df["score_masa"]      = _normalize(df["masa_salarial_win"].fillna(0))
    df["score_afiliados"] = _normalize(df["num_afiliados"].fillna(0))

    df["score_potencial"] = (
        df["score_segmento"]  * 0.40 +
        df["score_masa"]      * 0.35 +
        df["score_afiliados"] * 0.25
    ).round(4)

    print(f"  T8  Score potencial calculado — rango: "
          f"{df['score_potencial'].min():.3f} – {df['score_potencial'].max():.3f}")
    return df


def t9_metricas_asesor(df_master: pd.DataFrame) -> pd.DataFrame:
    """T9 — Calcula métricas de desempeño por asesor."""
    grp = df_master.groupby("id_posicion_ejecutivo")

    metricas = grp.agg(
        n_empresas          = ("emp_hash64",         "count"),
        n_premium           = ("desc_segmento_empleador_actual", lambda x: (x == "PREMIUM").sum()),
        total_afiliados     = ("num_afiliados",       "sum"),
        masa_salarial_total = ("masa_salarial_win",   "sum"),
        score_promedio      = ("score_potencial",     "mean"),
        empresas_dispersion = ("flag_dispersion",     "sum"),
    ).reset_index()

    # Tasa de retención (empresas sin dispersión / total con actividad PO)
    total_con_po = df_master.groupby("id_posicion_ejecutivo")["num_afiliados"].apply(
        lambda x: (x > 0).sum()
    ).reset_index(name="con_actividad_po")
    metricas = metricas.merge(total_con_po, on="id_posicion_ejecutivo", how="left")
    metricas["tasa_retencion"] = (
        (metricas["con_actividad_po"] - metricas["empresas_dispersion"])
        / metricas["con_actividad_po"].replace(0, np.nan)
    ).round(4)

    # Ranking compuesto
    metricas["rank_score"]     = metricas["score_promedio"].rank(ascending=False)
    metricas["rank_retencion"] = metricas["tasa_retencion"].rank(ascending=False, na_option="bottom")
    metricas["ranking_final"]  = (metricas["rank_score"] + metricas["rank_retencion"]).rank().astype(int)

    print(f"  T9  Métricas calculadas para {len(metricas):,} asesores")
    return metricas


# ── Orquestador de transformaciones ───────────────────────────────────────

def transform(raw: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    print("  --- Asignación ---")
    asig = raw["asignacion"].copy()
    asig = t1_dedup_asignacion(asig)
    asig = t2_nulos_lider(asig)
    asig = t4_excluir_periodo_futuro(asig)
    asig = t5_ejecutivos_historicos(asig, raw["jerarquia"])
    asig = t10_estandarizar_periodos(asig, "periodo_asignacion")
    asig_vigente = t11_asignacion_reciente(asig)

    print("  --- Monitoreo PO ---")
    mon = raw["monitoreo"].copy()
    mon = t3_winsorize_salario(mon)
    disp = t7_flag_dispersion(mon)

    # Agregados de monitoreo por empresa
    # Use only the latest monitoring cut for current portfolio metrics.
    # Keep the full `mon` frame above for the two-period dispersion analysis.
    latest_period = pd.to_numeric(mon["periodo"], errors="coerce").max()
    mon_latest = mon[pd.to_numeric(mon["periodo"], errors="coerce") == latest_period]
    mon_agg = mon_latest.groupby("emp_hash64").agg(
        num_afiliados    = ("afi_hash64",        "count"),
        masa_salarial_win= ("salario_tope_win",   "sum"),
    ).reset_index()

    print("  --- Integración ---")
    master = t6_integrar_fuentes(asig_vigente, raw["segmentacion"], mon_agg)
    master = master.merge(disp, on="emp_hash64", how="left")
    master["flag_dispersion"] = master["flag_dispersion"].fillna(0).astype(int)
    master["num_afiliados"]   = master["num_afiliados"].fillna(0)
    master["masa_salarial_win"] = master["masa_salarial_win"].fillna(0)

    print("  --- Score y métricas ---")
    master  = t8_score_potencial(master)
    asesores = t9_metricas_asesor(master)

    # Unir nombre del asesor
    asesores = asesores.merge(
        raw["jerarquia"][["idEjecutivo", "nombreEjecutivo", "descRegional", "descAgrupador"]].drop_duplicates("idEjecutivo"),
        left_on="id_posicion_ejecutivo", right_on="idEjecutivo", how="left"
    )

    return {
        "master_empresas": master,
        "metricas_asesores": asesores,
        "dispersion": disp,
    }


if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from extract.extract import extract
    print("=== TRANSFORMACIÓN (prueba directa) ===")
    raw = extract()
    processed = transform(raw)
    for k, v in processed.items():
        print(f"  {k}: {v.shape}")
