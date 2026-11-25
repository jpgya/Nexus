const EXP = new Uint8Array(512);
const LOG = new Uint8Array(256);

let value = 1;
for (let exponent = 0; exponent < 255; exponent += 1) {
  EXP[exponent] = value;
  LOG[value] = exponent;
  value <<= 1;
  if (value & 0x100) value ^= 0x11d;
}
for (let exponent = 255; exponent < EXP.length; exponent += 1) {
  EXP[exponent] = EXP[exponent - 255];
}

function multiply(a, b) {
  if (a === 0 || b === 0) return 0;
  return EXP[LOG[a] + LOG[b]];
}

function divide(a, b) {
  if (b === 0) throw new RangeError("Cannot divide by zero in GF(256)");
  if (a === 0) return 0;
  return EXP[(LOG[a] - LOG[b] + 255) % 255];
}

function invertMatrix(matrix) {
  const size = matrix.length;
  const augmented = matrix.map((row, index) => [
    Uint8Array.from(row),
    Uint8Array.from({ length: size }, (_, column) => Number(index === column))
  ]);

  for (let column = 0; column < size; column += 1) {
    let pivot = column;
    while (pivot < size && augmented[pivot][0][column] === 0) pivot += 1;
    if (pivot === size) throw new RangeError("Selected shards do not form an invertible matrix");
    [augmented[column], augmented[pivot]] = [augmented[pivot], augmented[column]];

    const pivotValue = augmented[column][0][column];
    for (let index = 0; index < size; index += 1) {
      augmented[column][0][index] = divide(augmented[column][0][index], pivotValue);
      augmented[column][1][index] = divide(augmented[column][1][index], pivotValue);
    }

    for (let row = 0; row < size; row += 1) {
      if (row === column) continue;
      const factor = augmented[row][0][column];
      if (factor === 0) continue;
      for (let index = 0; index < size; index += 1) {
        augmented[row][0][index] ^= multiply(factor, augmented[column][0][index]);
        augmented[row][1][index] ^= multiply(factor, augmented[column][1][index]);
      }
    }
  }
  return augmented.map((row) => row[1]);
}

function multiplyRow(row, matrix) {
  const result = new Uint8Array(matrix[0].length);
  for (let column = 0; column < result.length; column += 1) {
    for (let index = 0; index < row.length; index += 1) {
      result[column] ^= multiply(row[index], matrix[index][column]);
    }
  }
  return result;
}

function generatorMatrix(dataShards, totalShards) {
  if (!Number.isInteger(dataShards) || !Number.isInteger(totalShards)
    || dataShards < 1 || totalShards < dataShards || totalShards > 255) {
    throw new RangeError("Require 1 <= dataShards <= totalShards <= 255");
  }
  const vandermonde = Array.from({ length: totalShards }, (_, row) => {
    const x = row + 1;
    const values = new Uint8Array(dataShards);
    values[0] = 1;
    for (let column = 1; column < dataShards; column += 1) {
      values[column] = multiply(values[column - 1], x);
    }
    return values;
  });
  const topInverse = invertMatrix(vandermonde.slice(0, dataShards));
  return vandermonde.map((row) => multiplyRow(row, topInverse));
}

export function encodeReedSolomon(data, { dataShards, parityShards }) {
  if (!(data instanceof Uint8Array)) throw new TypeError("data must be a Uint8Array");
  const totalShards = dataShards + parityShards;
  const matrix = generatorMatrix(dataShards, totalShards);
  const shardSize = Math.max(1, Math.ceil(data.length / dataShards));
  const source = Array.from({ length: dataShards }, (_, index) => {
    const shard = new Uint8Array(shardSize);
    shard.set(data.subarray(index * shardSize, (index + 1) * shardSize));
    return shard;
  });

  return matrix.map((row) => {
    const shard = new Uint8Array(shardSize);
    for (let sourceIndex = 0; sourceIndex < dataShards; sourceIndex += 1) {
      const coefficient = row[sourceIndex];
      if (coefficient === 0) continue;
      for (let offset = 0; offset < shardSize; offset += 1) {
        shard[offset] ^= multiply(coefficient, source[sourceIndex][offset]);
      }
    }
    return shard;
  });
}

export function decodeReedSolomon(shards, {
  dataShards,
  totalShards,
  originalLength
}) {
  if (!Array.isArray(shards) || shards.length < dataShards) {
    throw new RangeError(`At least ${dataShards} distinct shards are required`);
  }
  if (!Number.isInteger(originalLength) || originalLength < 0) {
    throw new RangeError("originalLength must be a non-negative integer");
  }
  const seen = new Set();
  const selected = [];
  let shardSize;
  for (const shard of shards) {
    if (!Number.isInteger(shard.index) || shard.index < 0 || shard.index >= totalShards) {
      throw new RangeError("Shard index is outside the configured encoding");
    }
    if (!(shard.data instanceof Uint8Array)) throw new TypeError("Shard data must be a Uint8Array");
    if (seen.has(shard.index)) continue;
    if (shardSize !== undefined && shard.data.length !== shardSize) {
      throw new RangeError("Shard sizes do not match");
    }
    shardSize = shard.data.length;
    seen.add(shard.index);
    selected.push(shard);
    if (selected.length === dataShards) break;
  }
  if (selected.length < dataShards) {
    throw new RangeError(`At least ${dataShards} distinct shards are required`);
  }
  if (originalLength > dataShards * shardSize) {
    throw new RangeError("originalLength exceeds the decoded shard capacity");
  }

  const matrix = generatorMatrix(dataShards, totalShards);
  const selectedMatrix = selected.map((shard) => matrix[shard.index]);
  const inverse = invertMatrix(selectedMatrix);
  const data = new Uint8Array(dataShards * shardSize);
  for (let index = 0; index < dataShards; index += 1) {
    const decodedShard = new Uint8Array(shardSize);
    for (let sourceIndex = 0; sourceIndex < dataShards; sourceIndex += 1) {
      const coefficient = inverse[index][sourceIndex];
      if (coefficient === 0) continue;
      for (let offset = 0; offset < shardSize; offset += 1) {
        decodedShard[offset] ^= multiply(coefficient, selected[sourceIndex].data[offset]);
      }
    }
    data.set(decodedShard, index * shardSize);
  }
  return data.slice(0, originalLength);
}
