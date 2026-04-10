from datetime import datetime

import psycopg2

from config import load_settings

RAW_LINES = """
Alexandro;08/04/2026;5541996453529
Carlos NHS;08/04/2026;5541998191124
Joelma;08/04/2026;5541997071645
Mariana;09/04/2026;5541991110136
Matheus Galdino;09/04/2026;5541984231394
Amarildo;10/04/2026;5541998575170
Bruno;10/04/2026;5541995232345
Celso;10/04/2026;5541999841584
Eric Lopes;10/04/2026;5541984096904
Guilherme Hoip;10/04/2026;5541996409638
Jessica;10/04/2026;5541997238359
Kelvin;10/04/2026;5541987995407
Lucas;10/04/2026;5541997208292
Lucena;10/04/2026;5541997635130
Nadir;10/04/2026;5541998468310
Liliane;11/04/2026;5541997239339
Nei Pacco;11/04/2026;5541988760553
Thon;11/04/2026;5541997362930
Cristiano;15/04/2026;5541988881967
Jeanderson;15/04/2026;5541998575170
Luana;15/04/2026;5541999479995
Reinaldo;15/04/2026;5541999825478
Ton;15/04/2026;5541998333092
Vinicius Staron;15/04/2026;5541996888936
Willian Botelho;15/04/2026;5541995875261
Cleci;16/04/2026;5541996732614
Ney;16/04/2026;5541995431864
Cezar da Silva;18/04/2026;5541996285849
Luciano Roberto;18/04/2026;5541995577995
Leandro Canedo;20/04/2026;5541999036392
Lorena;20/04/2026;5541998636205
Jair correia;20/04/2026;5541996763332
Jaqueline Pereira;22/04/2026;5541997088829
Vivi Guedes;22/04/2026;5541999939234
Everton;23/04/2026;5541998866886
Patricia Almeida;23/04/2026;5541987796576
Luk;24/04/2026;5541997403807
Bruna;25/04/2026;5541996374699
Gabriel Barbosa;25/04/2026;5541987859959
Rogerio ( Antonio Carlos);25/04/2026;5541998484970
Guilherme Ferreira;26/04/2026;5541997091083
Paloma Benete;26/04/2026;5541998908859
Ricardo;26/04/2026;5541997588567
Beti;28/04/2026;5541992031544
Nelci ( Bruna);28/04/2026;5541996374699
Beto;01/05/2026;5541996182019
Guilherme Castelan;01/05/2026;5541995758296
Guina;01/05/2026;5541995662004
Josiliane;01/05/2026;5541997612068
Marilim;01/05/2026;5541991091696
Milena;01/05/2026;5541999818533
Paulinho;01/05/2026;5541997318511
Rosane;01/05/2026;5541999943202
Carlos Roberto;02/05/2026;5541995563074
Julio;02/05/2026;5541998841081
Mauricio;02/05/2026;5541997088829
Danielly;03/05/2026;5541999878064
Mari;03/05/2026;5541998798606
Luciane C;03/05/2026;5541997535910
Rafael;05/05/2026;5541997537674
Cleverson;05/05/2026;5541997269111
Giovane;05/05/2026;5541995131813
Jhennifer;05/05/2026;5541988338062
Joel;05/05/2026;5541997114215
Luis St;05/05/2026;5541991532017
Thays Pien;05/05/2026;5541999330207
Silmara;05/05/2026;5541999403223
Francisco chataing;05/05/2026;5541995115744
Gisele;05/05/2026;5541987116087
Layara;05/05/2026;5541998551448
Lilydhiow;05/05/2026;5541999401997
Mariana Maah;05/05/2026;5547989033663
Adriane dos Santos;06/05/2026;5541996496494
Dorielton;06/05/2026;5543999832113
Maike;06/05/2026;5541991713592
Osmar;06/05/2026;5541985246000
Andre Modesto;06/05/2026;5541996834156
Cesar;06/05/2026;5541997708814
Fernando;06/05/2026;5541999251627
Mari;06/05/2026;5541995244916
Jorge;07/05/2026;5541991110136
David;08/05/2026;5541997312014
Gabrielle;08/05/2026;5541995244916
Candido;08/05/2026;5541985248540
Jaqueline de Fatima;08/05/2026;5541999760214
Juliana;08/05/2026;5541988760553
Mayara;08/05/2026;5541999608456
Nathan;10/05/2026;5541988081419
Paloma de Souza;10/05/2026;5541992031544
Ismar;13/05/2026;5541988799513
Eliane;15/05/2026;5541984096904
Luciane Peixe;15/05/2026;5541998445223
Marcelo Cardenaz;20/05/2026;5541988344004
Cicero;27/05/2026;5541996762474
Nelson;05/06/2026;5541995193075
Katia;06/06/2026;5541999298222
Anderson Pieri;07/06/2026;5541998468310
Leandro Ramos;10/06/2026;5541991713592
Rogerio e Mae;17/06/2026;5541997403807
Gerino;21/06/2026;5541992465090
Willian;26/06/2026;5541997799453
Polak;05/07/2026;5541991854064
Joana;23/07/2026;5541997290625
Alexandre;03/08/2026;5541999457417
Eva;12/01/2027;5541996810204
Cristian Gualdezi;17/01/2027;5541998815734
Marcos Luciano;19/02/2027;5541992266088
Agnaldo;20/06/2027;5541984412591
Sergio;07/10/2027;5541991233999
Sirlei;13/10/2028;5541992668693
"""


def parse_updates() -> dict[str, str]:
    updates: dict[str, str] = {}
    for line in RAW_LINES.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(";")]
        if len(parts) < 3:
            continue
        due_date = parts[1]
        number = parts[2]
        updates[number] = due_date
    return updates


def main() -> None:
    settings = load_settings()
    if not settings.database_url:
        raise RuntimeError("DATABASE_URL nao configurada")

    updates = parse_updates()
    timestamp = datetime.utcnow().strftime("%d/%m/%Y %H:%M:%S")

    with psycopg2.connect(settings.database_url) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT numero_original, numero_novo FROM numeros_override")
            override_rows = cursor.fetchall()

            original_to_current = {str(orig): str(new) for orig, new in override_rows if orig and new}
            current_to_original = {str(new): str(orig) for orig, new in override_rows if orig and new}

            panel_updated = 0
            override_upserts = 0
            not_found_in_panel: list[str] = []

            for provided_number, due_date in updates.items():
                current_number = original_to_current.get(provided_number, provided_number)
                original_number = current_to_original.get(current_number, provided_number)

                cursor.execute(
                    """
                    UPDATE clientes_painel
                    SET vencimento = %s, atualizado_em = %s
                    WHERE numero = %s
                    """,
                    (due_date, timestamp, current_number),
                )
                rowcount = cursor.rowcount

                if rowcount == 0 and original_number != current_number:
                    cursor.execute(
                        """
                        UPDATE clientes_painel
                        SET vencimento = %s, atualizado_em = %s
                        WHERE numero = %s
                        """,
                        (due_date, timestamp, original_number),
                    )
                    rowcount = cursor.rowcount

                if rowcount > 0:
                    panel_updated += rowcount
                else:
                    not_found_in_panel.append(provided_number)

                cursor.execute(
                    """
                    INSERT INTO vencimentos_override (numero, vencimento, atualizado_em)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (numero) DO UPDATE SET
                        vencimento = excluded.vencimento,
                        atualizado_em = excluded.atualizado_em
                    """,
                    (original_number, due_date, timestamp),
                )
                override_upserts += 1

        conn.commit()

    print(f"Linhas recebidas: {len([l for l in RAW_LINES.splitlines() if l.strip()])}")
    print(f"Numeros unicos aplicados: {len(updates)}")
    print(f"Clientes atualizados em clientes_painel: {panel_updated}")
    print(f"Upserts em vencimentos_override: {override_upserts}")
    print(f"Nao encontrados em clientes_painel: {len(not_found_in_panel)}")
    if not_found_in_panel:
        sample = ", ".join(not_found_in_panel[:20])
        print(f"Exemplos nao encontrados: {sample}")


if __name__ == "__main__":
    main()
