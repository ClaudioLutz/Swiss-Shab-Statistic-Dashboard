
import matplotlib
# Use Agg backend for non-interactive plotting
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import logging
import os

logger = logging.getLogger(__name__)

def generate_plots(df, start_date, end_date, output_dir='./static'):
    """
    Generate all plots for the dashboard.

    Args:
        df: DataFrame containing the SHAB data.
        start_date: Start date of the range (date object)
        end_date: End date of the range (date object)
        output_dir: Directory to save plots.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    logger.info("Generating plots...")

    if df.empty:
        logger.warning("DataFrame is empty. Skipping plot generation.")
        return

    # Pre-process data for plotting
    df['date'] = pd.to_datetime(df['date'])
    df['month'] = df['date'].dt.to_period('M') # Use period for sorting

    # 1. FacetGrid per Kanton
    try:
        grouped_multiple = df.groupby(['month', 'subrubric', 'kanton']).agg({'subrubric': ['count']})
        grouped_multiple.columns = ['count']
        grouped_multiple = grouped_multiple.reset_index()

        # Convert month back to string for plotting but ensure order?
        # Seaborn might plot strings in order of appearance or alphabetical.
        # Better to convert to timestamp for x-axis or sort explicitly.
        grouped_multiple['month_str'] = grouped_multiple['month'].dt.strftime('%Y-%m')
        grouped_multiple = grouped_multiple.sort_values('month')

        logger.info("Generating FacetGridKanton...")
        # Sort data to ensure months are in order
        grouped_multiple.sort_values(by=['kanton', 'month'], inplace=True)

        g = sns.FacetGrid(grouped_multiple, col="kanton", col_wrap=5, hue="subrubric", sharey=False)
        g.map(sns.lineplot, "month_str", "count")
        g.add_legend()
        g.set_axis_labels(f"{start_date} - {end_date}", "Meldungen")
        g.set(xticklabels=[]) # Hide x labels to avoid clutter

        output_path = os.path.join(output_dir, "FacetGridKanton.png")
        g.savefig(output_path)
        plt.close()
        logger.info(f"Saved {output_path}")

    except Exception as e:
        logger.error(f"Failed to generate FacetGridKanton: {e}")

    # 2. LineGraph (Total without Kantons)
    try:
        grouped_no_kanton = df.groupby(['month', 'subrubric']).agg({'subrubric': ['count']})
        grouped_no_kanton.columns = ['count']
        grouped_no_kanton = grouped_no_kanton.reset_index()
        grouped_no_kanton['month_str'] = grouped_no_kanton['month'].dt.strftime('%Y-%m')
        grouped_no_kanton = grouped_no_kanton.sort_values('month')

        logger.info("Generating LineGraph...")
        plt.figure(figsize=(20, 6))
        sns.set_style(style='darkgrid')

        # Explicitly pass ax to avoid variable shadowing issues if we used subplots
        ax = sns.lineplot(data=grouped_no_kanton, x="month_str", y='count', hue='subrubric')
        plt.xticks(rotation=45)
        plt.title(f"SHAB Meldungen {start_date} - {end_date}")

        output_path = os.path.join(output_dir, "LineGraph.png")
        plt.savefig(output_path, bbox_inches='tight')
        plt.close()
        logger.info(f"Saved {output_path}")

    except Exception as e:
        logger.error(f"Failed to generate LineGraph: {e}")
