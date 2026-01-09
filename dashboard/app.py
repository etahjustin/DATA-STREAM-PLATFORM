import time
import pandas as pd
import streamlit as st
import s3fs

st.set_page_config(
    page_title="Bank Data Monitor",
    page_icon="",
    layout="wide"
)

MINIO_OPTS = {
    "key": "minio_access_key",
    "secret": "minio_secret_key",
    "client_kwargs": {"endpoint_url": "http://localhost:9000"},
    "use_listings_cache": False 
}
DATA_PATH = "s3://datalake/tables/transactions"

def load_data():
    """Lit les données Delta/Parquet directement depuis MinIO"""
    try:
        # On utilise s3fs pour lister et lire
        fs = s3fs.S3FileSystem(**MINIO_OPTS)
        
        if not fs.exists(DATA_PATH):
            return pd.DataFrame() 
            
        df = pd.read_parquet(DATA_PATH, storage_options=MINIO_OPTS)
        
        if not df.empty and 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values(by='timestamp', ascending=False)
            
        return df
    except Exception as e:
        return pd.DataFrame()


st.title(" Monitoring")
st.markdown("---")

placeholder = st.empty()

while True:
    df = load_data()

    with placeholder.container():
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)

        if df.empty:
            st.warning("En attente de données dans le Data Lake...")
        else:
            # Calcul des métriques
            total_tx = len(df)
            total_vol = df['amount'].sum()
            last_tx_time = df['timestamp'].iloc[0].strftime('%H:%M:%S')
            avg_amount = df['amount'].mean()

            # Affichage des KPIs
            kpi1.metric(label="Transactions Totales", value=total_tx, delta="Live")
            kpi2.metric(label="Volume Total (€)", value=f"{total_vol:,.2f} €")
            kpi3.metric(label="Moyenne / Tx", value=f"{avg_amount:.2f} €")
            kpi4.metric(label="Dernière Synchro", value=last_tx_time)

            # Ligne : Graphiques
            st.markdown("### Analyse des Flux")
            col_chart1, col_chart2 = st.columns(2)

            with col_chart1:
                st.caption("Volume de transactions par minute")
                if 'timestamp' in df.columns:
                    df_resampled = df.set_index('timestamp').resample('1min')['amount'].count()
                    st.line_chart(df_resampled)

            with col_chart2:
                st.caption("Distribution des Statuts")
                status_counts = df['status'].value_counts()
                st.bar_chart(status_counts)

            
            st.markdown("###  Dernières Écritures (Audit Log)")
            st.dataframe(
                df[['event_id', 'timestamp', 'user_id', 'amount', 'currency', 'status']].head(10),
                use_container_width=True,
                hide_index=True
            )

    time.sleep(2)