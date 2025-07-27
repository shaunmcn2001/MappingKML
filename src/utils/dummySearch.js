export async function dummySearch(input) {
  const lines = input.split(/\n/).map(l => l.trim()).filter(Boolean);
  const results = lines.map((line, idx) => {
    const [lot, plan] = line.split('/');
    return { id: idx, lot: lot || '', plan: plan || '' };
  });
  return new Promise(resolve => setTimeout(() => resolve(results), 300));
}
