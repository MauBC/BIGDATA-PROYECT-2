"""Run one loader for the published sample; dependencies run in Docker."""
import argparse


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('library', choices=['datasets', 'pandas', 'duckdb'])
    parser.add_argument('--repo', required=True, help='HF dataset ID: mashu-data/repository-name')
    args = parser.parse_args()
    if args.library == 'datasets':
        from datasets import load_dataset
        comments = load_dataset(args.repo, 'comments', split='train', streaming=True)
        print(next(iter(comments)))
    elif args.library == 'pandas':
        import pandas as pd
        # Read one real shard, not the full dataset, into memory.
        url = f'https://huggingface.co/datasets/{args.repo}/resolve/main/data/comments/subreddit=LocalLLaMA/part-0.parquet'
        comments = pd.read_parquet(url)
        print(comments[['id', 'link_id', 'parent_id', 'depth']].head())
    else:
        import duckdb
        from huggingface_hub import hf_hub_download
        # Download one shard to the HF cache, then query the local Parquet file.
        path = hf_hub_download(repo_id=args.repo, repo_type='dataset',
            filename='data/comments/subreddit=LocalLLaMA/part-0.parquet')
        conn = duckdb.connect()
        rows = conn.execute('SELECT id, link_id, parent_id, depth FROM read_parquet(?, hive_partitioning=false) LIMIT 20', [path]).fetchall()
        print(rows)


if __name__ == '__main__':
    main()
