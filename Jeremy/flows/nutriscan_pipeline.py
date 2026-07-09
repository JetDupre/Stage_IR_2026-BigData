from prefect import flow, task
import subprocess
import duckdb
from datetime import datetime

@task(name='dbt-run')
def run_dbt():
    print("Lancement de dbt run...")
    # L'ajout de shell=True permet à Windows de localiser correctement le script dbt dans le venv
    result = subprocess.run(
        ['dbt', 'run', '--project-dir', 'C:/data/nutriScan/nutriscan_dbt'],
        capture_output=True, text=True, shell=True
    )
    if result.returncode != 0:
        # En combinant stdout et stderr, on s'assure d'obtenir le message d'erreur dbt réel
        error_msg = result.stderr if result.stderr else result.stdout
        raise Exception(f'dbt run failed: {error_msg}')
    print(result.stdout)

@task(name='dbt-test')
def run_dbt_test():
    print("Lancement de dbt test...")
    result = subprocess.run(
        ['dbt', 'test', '--project-dir', 'C:/data/nutriScan/nutriscan_dbt'],
        capture_output=True, text=True, shell=True
    )
    if result.returncode != 0:
        error_msg = result.stderr if result.stderr else result.stdout
        print(f"dbt test a détecté des anomalies de qualité (comportement attendu) :\n{error_msg}")
        return False
    print(result.stdout)
    return True

@task(name='dbt-docs')
def run_dbt_docs():
    print("Génération de la documentation dbt...")
    result = subprocess.run(
        ['dbt', 'docs', 'generate', '--project-dir', 'C:/data/nutriScan/nutriscan_dbt'],
        capture_output=True, text=True, shell=True
    )
    if result.returncode != 0:
        error_msg = result.stderr if result.stderr else result.stdout
        raise Exception(f'dbt docs generate failed: {error_msg}')
    print(result.stdout)

@task(name='log-pipeline')
def log_pipeline(test_passed: bool):
    # Connexion à la base DuckDB locale de NutriScan
    conn = duckdb.connect('C:/data/nutriScan/nutriscan_dbt/nutriScan.duckdb')
    
    # Sécurité : Création automatique de la table d'audit si elle n'existe pas
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_logs (
            run_id VARCHAR,
            run_date VARCHAR,
            source_file VARCHAR,
            output_file VARCHAR,
            nb_lignes_output INTEGER,
            statut VARCHAR
        );
    """)
    
    # Préparation des variables demandées par le workshop
    now = datetime.now()
    run_id = 'DBT_' + now.strftime('%Y%m%d_%H%M%S')
    run_date = now.strftime('%Y-%m-%d %H:%M:%S')
    statut = 'SUCCESS' if test_passed else 'FAILED'
    
    # Insertion de la ligne de log dbt dans la table d'audit
    conn.execute(f"""
        INSERT INTO pipeline_logs (run_id, run_date, source_file, output_file, nb_lignes_output, statut)
        VALUES ('{run_id}', '{run_date}', 'dbt run', 'gold/', 0, '{statut}')
    """)
    conn.close()
    print(f"Audit log inséré dans DuckDB avec le run_id : {run_id} | Statut : {statut}")

@flow(name='NutriScan-Pipeline')
def nutriscan_pipeline():
    # 1. Transformation des données (Silver -> Gold)
    run_dbt()
    
    # 2. Exécution des tests automatisés de Data Quality
    test_passed = run_dbt_test()
    
    # 3. Génération automatique du catalogue de données
    run_dbt_docs()
    
    # 4. Enregistrement du rapport d'exécution dans la table d'audit
    log_pipeline(test_passed)

if __name__ == '__main__':
    nutriscan_pipeline()