//! Pool falso com o inbox transacional: a linha reivindicada só vale depois do
//! COMMIT, e o ROLLBACK a desfaz. Sem banco.

function poolFalso() {
  const inbox = new Set();
  const comandos = [];
  let liberados = 0;
  const pool = {
    inbox,
    comandos,
    liberados: () => liberados,
    query: async () => ({ rowCount: 0, rows: [] }),
    connect: async () => {
      let pendente = null;
      return {
        query: async (sql, params) => {
          comandos.push(sql);
          if (sql === 'COMMIT') {
            if (pendente) inbox.add(pendente);
            pendente = null;
          } else if (sql === 'ROLLBACK') {
            pendente = null;
          } else if (sql.startsWith('INSERT INTO uhura_inbox')) {
            const id = params[0];
            if (inbox.has(id)) return { rowCount: 0 };
            pendente = id;
            return { rowCount: 1 };
          }
          return { rowCount: 1, rows: [{ id: '1' }] };
        },
        release: () => {
          liberados += 1;
        },
      };
    },
  };
  return pool;
}

module.exports = { poolFalso };
