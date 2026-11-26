"""Prove lossless integer narrowing from every row group's physical INT64 statistics."""
import json
from pathlib import Path

INT32_MIN = -(2**31)
INT32_MAX = 2**31 - 1


def inspect(spark, folder, columns):
    jvm = spark._jvm
    files = sorted(Path(folder).glob('*.parquet'))
    if not files:
        raise ValueError('No Parquet files for integer-domain proof')
    bounds = {c: {'minimum': None, 'maximum': None, 'row_groups': 0} for c in columns}
    inventory = []
    for path in files:
        stat = path.stat()
        footer = jvm.org.apache.parquet.hadoop.ParquetFileReader.readFooter(
            spark.sparkContext._jsc.hadoopConfiguration(), jvm.org.apache.hadoop.fs.Path(str(path)))
        for block in footer.getBlocks():
            found = set()
            for chunk in block.getColumns():
                name = str(chunk.getPath().toDotString())
                if name not in bounds:
                    continue
                found.add(name)
                if str(chunk.getPrimitiveType().getPrimitiveTypeName()) != 'INT64':
                    raise ValueError('Narrowing requires a physical INT64 column: ' + name)
                stats = chunk.getStatistics()
                value = bounds[name]
                value['row_groups'] += 1
                if stats.isNumNullsSet() and stats.getNumNulls() == block.getRowCount():
                    continue
                if not stats.hasNonNullValue():
                    raise ValueError('Missing integer bounds: ' + str(path) + ':' + name)
                low, high = int(stats.genericGetMin()), int(stats.genericGetMax())
                if not INT32_MIN <= low <= high <= INT32_MAX:
                    raise ValueError('Integer domain exceeds INT32: ' + str((name, low, high)))
                value['minimum'] = low if value['minimum'] is None else min(value['minimum'], low)
                value['maximum'] = high if value['maximum'] is None else max(value['maximum'], high)
            if found != set(columns):
                raise ValueError('Missing columns in row-group metadata')
        inventory.append({'path': str(path), 'size': stat.st_size, 'mtime_ns': stat.st_mtime_ns})
    return {'columns': bounds, 'files': inventory}


def require_unchanged(work, dataset, required):
    proof = json.loads((Path(work)/'evidence/integer-domains.json').read_text())
    if proof['dataset'] != dataset or not proof['boundary_checks_passed']:
        raise ValueError('Integer proof does not match this dataset')
    for table, columns in required.items():
        entry = proof['tables'][table]
        actual = {str(p) for p in (Path(dataset)/table).glob('*.parquet')}
        if actual != {f['path'] for f in entry['files']}:
            raise ValueError('Dataset file inventory changed after integer proof')
        for f in entry['files']:
            stat = Path(f['path']).stat()
            if (stat.st_size, stat.st_mtime_ns) != (f['size'], f['mtime_ns']):
                raise ValueError('Dataset file changed after integer proof')
        for column in columns:
            domain = entry['columns'][column]
            if not domain['row_groups'] or (domain['minimum'] is not None and
                    not INT32_MIN <= domain['minimum'] <= domain['maximum'] <= INT32_MAX):
                raise ValueError('Invalid integer-domain proof')
    return proof


def main():
    import argparse
    from pyspark.sql import SparkSession
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work', required=True)
    args = parser.parse_args()
    work = Path(args.work).resolve()
    target = work/'evidence/integer-domains.json'
    if target.exists():
        raise RuntimeError('Integer-domain evidence exists')
    spark = SparkSession.builder.appName('b200-integer-domain-audit').getOrCreate()
    try:
        tests = work/'scratch/integer-domain-boundaries'
        for name, rows, valid in [
                ('edges', [(INT32_MIN,), (INT32_MAX,), (None,)], True),
                ('overflow', [(INT32_MAX+1,)], False),
                ('underflow', [(INT32_MIN-1,)], False)]:
            path = tests/name
            spark.createDataFrame(rows, 'key long').coalesce(1).write.parquet(str(path))
            try:
                inspect(spark, path, ['key'])
            except ValueError:
                if valid:
                    raise
            else:
                if not valid:
                    raise AssertionError('Out-of-range Parquet input was accepted')
        dataset = '/raid/knataraj/datasets/tpch/sf10k_v2_float'
        tables = {}
        for table, columns in {'lineitem': ['l_suppkey','l_partkey'],
                               'partsupp': ['ps_suppkey','ps_partkey'],
                               'supplier': ['s_suppkey'], 'part': ['p_partkey']}.items():
            tables[table] = inspect(spark, Path(dataset)/table, columns)
            print(json.dumps({'table': table, 'bounds': tables[table]['columns'],
                              'files': len(tables[table]['files'])}), flush=True)
        target.write_text(json.dumps({'dataset': dataset, 'boundary_checks_passed': True,
                                      'tables': tables}, indent=2)+'\n')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
