import pandas as pd

file_path = "crimes-et-delits-enregistres-par-les-services-de-gendarmerie-et-de-police-depuis-2012.xlsx"


def detect_unite_row(df, keyword):
    for idx, row in df.iterrows():
        values = row.astype(str).str.strip()
        count = values.str.startswith(keyword).sum()
        if count >= 3:
            return idx
    return None


def build_base(file_path, service_label):

    xls = pd.ExcelFile(file_path)
    sheets = [s for s in xls.sheet_names if f"Services {service_label}" in s]

    all_data = []

    for sheet in sheets:
        print(f"Traitement : {sheet}")

        annee = int(sheet.split()[-1])
        df_raw = pd.read_excel(file_path, sheet_name=sheet, header=None)
        df_raw = df_raw.dropna(how="all")

        # Ligne Départements
        dept_row = df_raw[df_raw.apply(
            lambda r: r.astype(str).str.contains("Départements", na=False).any(),
            axis=1
        )].index[0]

        departements = df_raw.iloc[dept_row]

        if service_label == "PN":

            # Ligne Périmètres
            perim_row = df_raw[df_raw.apply(
                lambda r: r.astype(str).str.contains("Périmètres", na=False).any(),
                axis=1
            )].index[0]

            perimetres = df_raw.iloc[perim_row]
            unite_row = detect_unite_row(df_raw, "CSP")
            unites = df_raw.iloc[unite_row]

        else:  # GN

            perimetres = None
            unite_row = detect_unite_row(df_raw, "CGD")
            unites = df_raw.iloc[unite_row]

        # Ligne Code index
        header_row = df_raw[df_raw.apply(
            lambda r: r.astype(str).str.contains("Code index", na=False).any(),
            axis=1
        )].index[0]

        df = df_raw.iloc[header_row+1:].reset_index(drop=True)
        df.columns = df_raw.iloc[header_row]
        df.columns = df.columns.astype(str).str.strip()

        code_col = [c for c in df.columns if "Code" in c][0]
        libelle_col = [c for c in df.columns if "Libell" in c][0]

        df = df.rename(columns={
            code_col: "code_index",
            libelle_col: "libelle_index"
        })

        libelle_pos = list(df.columns).index("libelle_index")
        col_indices = list(range(libelle_pos + 1, len(df.columns)))

        records = []

        for col_idx in col_indices:

            departement = str(departements[col_idx]).strip()

            if departement == "" or departement.lower() == "nan":
                continue

            temp = df[["code_index", "libelle_index"]].copy()
            temp["nb_fait"] = df.iloc[:, col_idx]
            temp["departement"] = departement

            if service_label == "PN":
                temp["perimetre"] = str(perimetres[col_idx]).strip()
            else:
                temp["perimetre"] = None  # GN n'a pas de périmètre

            temp["unite"] = str(unites[col_idx]).strip()
            temp["annee"] = annee
            temp["service"] = service_label

            records.append(temp)

        df_long = pd.concat(records, ignore_index=True)

        df_long["nb_fait"] = pd.to_numeric(df_long["nb_fait"], errors="coerce")
        df_long = df_long.dropna(subset=["nb_fait"])

        df_long = df_long[
            ["annee", "service", "departement",
             "perimetre", "unite",
             "libelle_index", "code_index", "nb_fait"]
        ]

        all_data.append(df_long)

    return pd.concat(all_data, ignore_index=True)


# Construction
base_PN = build_base(file_path, "PN")
base_GN = build_base(file_path, "GN")

base_finale = pd.concat([base_PN, base_GN], ignore_index=True)

# Nettoyage final
base_finale["departement"] = base_finale["departement"].astype(str).str.zfill(2)
base_finale["annee"] = base_finale["annee"].astype(int)
base_finale["nb_fait"] = base_finale["nb_fait"].astype(int)

base_finale = base_finale.reset_index(drop=True)

base_finale.to_csv("base_complete_PN_GN_structured.csv", index=False)

print("\n BASE COMPLETE PROPRE")
print("Nombre de lignes :", len(base_finale))
