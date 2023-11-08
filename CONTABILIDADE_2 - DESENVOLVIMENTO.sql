select tipo_acao, count(*) from TCB_CONTR_FILA_EVENTOS group BY tipo_acao;
select * from TCB_CONTR_FILA_EVENTOS where tipo_acao = 'BDT_FAT_GR';


DECLARE
  vProcessou VARCHAR2(1);
  vQtde_Ok NUMBER;
  vQtde_Erro NUMBER;
  CURSOR c_Dados IS SELECT *
                    FROM TCB_CONTR_FILA_EVENTOS
                    WHERE TIPO_ACAO = 'LOG_FAT';
  v_Dados c_Dados%ROWTYPE;
  
  PROCEDURE GRAVA_MENSAGEM(pMensagem VARCHAR2) IS
  BEGIN
    INSERT INTO TS_GRAVA_MENSAGEM (TS_MENSAGEM,MENSAGEM) VALUES (localtimestamp, pMensagem);
    COMMIT;
  END;
BEGIN
  DELETE FROM TS_GRAVA_MENSAGEM;
  COMMIT;
  vQtde_Ok := 0;
  vQtde_Erro := 0;
  OPEN c_Dados;
  LOOP
    FETCH c_Dados INTO v_Dados;
    EXIT WHEN c_Dados%NOTFOUND;
    PCB_CONSOME_FILA_EVENTOS(vProcessou);
    IF vProcessou = 'S'
        THEN vQtde_Ok := vQtde_Ok + 1;
    
    ELSIF vProcessou = 'S'
        THEN vQtde_Erro := vQtde_Erro + 1;
    
    end if;
  END LOOP;
  CLOSE c_Dados;
    GRAVA_MENSAGEM('ok:     ' || vQtde_ok);
    GRAVA_MENSAGEM('Erro:   ' || vQtde_Erro);
END;









DECLARE
  vProcessou VARCHAR2(1);
  vQtde_Ok NUMBER;
  vQtde_Erro NUMBER;
  CURSOR c_Dados IS SELECT *
                    FROM TCB_CONTR_FILA_EVENTOS
                    WHERE STATUS = 'E'
                      AND ROWNUM <= 1;
  v_Dados c_Dados%ROWTYPE;
  
  PROCEDURE GRAVA_MENSAGEM(pMensagem VARCHAR2) IS
  BEGIN
    INSERT INTO TS_GRAVA_MENSAGEM (TS_MENSAGEM,MENSAGEM) VALUES (localtimestamp, pMensagem);
    COMMIT;
  END;
BEGIN
  DELETE FROM TS_GRAVA_MENSAGEM;
  COMMIT;
  vQtde_Ok := 0;
  vQtde_Erro := 0;
  OPEN c_Dados;
  LOOP
    FETCH c_Dados INTO v_Dados;
    EXIT WHEN c_Dados%NOTFOUND;
        IF v_dados.TIPO_ACAO = 'BDT_FAT_GR' AND MENS_ERRO = 'Ficha financeira do previsto não encontrada!'
        THEN DELETE FROM TCB_CONTR_FILA_EVENTOS
             WHERE STATUS = 'E'
               AND TIPO_ACAO = 'BDT_FAT_GR'
               AND MENS_ERRO = 'Ficha financeira do previsto não encontrada!'
               AND COD_PERIODO   = v_Dados.COD_PERIODO
               AND COD_PESSOA    = v_Dados.COD_PESSOA
               AND COD_FIP_GF    = v_Dados.COD_FIP_GF
               AND COD_GRUPO_FIN = v_Dados.COD_GRUPO_FIN
               AND COD_SERVICO   = v_Dados.COD_SERVICO
               AND COD_PARCELA   = v_Dados.COD_PARCELA;
        END IF;
  END LOOP;
  CLOSE c_Dados;
    GRAVA_MENSAGEM('ok:     ' || vQtde_ok);
    GRAVA_MENSAGEM('Erro:   ' || vQtde_Erro);
END;
/









DECLARE
  vProcessou VARCHAR2(1);
  vQtde_Ok NUMBER;
  vQtde_Erro NUMBER;
  CURSOR c_Dados IS SELECT *
                    FROM TCB_CONTR_FILA_EVENTOS
                    WHERE TIPO_ACAO = 'FATURAMENTO';
  v_Dados c_Dados%ROWTYPE;
  
  PROCEDURE GRAVA_MENSAGEM(pMensagem VARCHAR2) IS
  BEGIN
    INSERT INTO TS_GRAVA_MENSAGEM (TS_MENSAGEM,MENSAGEM) VALUES (localtimestamp, pMensagem);
    COMMIT;
  END;
BEGIN
  DELETE FROM TS_GRAVA_MENSAGEM;
  COMMIT;
  vQtde_Ok := 0;
  vQtde_Erro := 0;
  OPEN c_Dados;
  LOOP
    FETCH c_Dados INTO v_Dados;
    EXIT WHEN c_Dados%NOTFOUND;
    PCB_CONSOME_FILA_EVENTOS(vProcessou);
    IF vProcessou = 'S'
        THEN vQtde_Ok := vQtde_Ok + 1;
    
    ELSIF vProcessou = 'S'
        THEN vQtde_Erro := vQtde_Erro + 1;
    
    end if;
  END LOOP;
  CLOSE c_Dados;
    GRAVA_MENSAGEM('ok:     ' || vQtde_ok);
    GRAVA_MENSAGEM('Erro:   ' || vQtde_Erro);
END;
/











SELECT * FROM ALL_SOURCE
WHERE TEXT LIKE '%CONF_FAT%';

SELECT * FROM tcb_contr_fila_eventos
WHERE TIPO_ACAO = 'CONF_FAT';


PCB_CONF_FAT_COM_FICHA