PCB_CONSOME_FILA_EVENTOS;

SELECT COUNT(*) FROM TCB_CONTR_FILA_EVENTOS
WHERE TIPO_ACAO = 'BDT_FAT_GR';


SELECT * FROM TCB_CONTR_FILA_EVENTOS
WHERE TIPO_ACAO = 'BDT_FAT_GR'
  AND TO_CHAR(FS_GET_DTH_ID_ALT(ID_EVENTO),'MMDD') >= TO_CHAR(SYSDATE-1,'MMDD') ;

DELETE FROM TCB_CONTR_FILA_EVENTOS
WHERE NOT(     TIPO_ACAO = 'BDT_FAT_GR'
           AND TO_CHAR(FS_GET_DTH_ID_ALT(ID_EVENTO),'MMDD') >= TO_CHAR(SYSDATE-1,'MMDD')
         );

DELETE FROM TCB_CONTR_FILA_EVENTOS
WHERE (TIPO_ACAO <> 'BDT_FAT_GR'
       OR TO_CHAR(FS_GET_DTH_ID_ALT(ID_EVENTO),'MMDD') < TO_CHAR(SYSDATE-1,'MMDD')
         );

SELECT COUNT(*) FROM TCB_CONTR_FILA_EVENTOS
WHERE TIPO_ACAO = 'BDT_FAT_GR';

DROP TABLE TS_GRAVA_MENSAGEM;
CREATE TABLE TS_GRAVA_MENSAGEM (TS_MENSAGEM TIMESTAMP,MENSAGEM VARCHAR2(4000));
DECLARE
  vQtde NUMBER;
  CURSOR c_Dados IS SELECT *
                    FROM TCB_CONTR_FILA_EVENTOS
                    WHERE ROWNUM <= 1;
  v_Dados c_Dados%ROWTYPE;
  
  PROCEDURE GRAVA_MENSAGEM(pMensagem VARCHAR2) IS
  BEGIN
    INSERT INTO TS_GRAVA_MENSAGEM (TS_MENSAGEM,MENSAGEM) VALUES (localtimestamp, pMensagem);
    COMMIT;
  END;
BEGIN
  DELETE FROM TS_GRAVA_MENSAGEM;
  COMMIT;
  vQtde := 0;
  OPEN c_Dados;
  LOOP
    FETCH c_Dados INTO v_Dados;
    EXIT WHEN c_Dados%NOTFOUND;
    vQtde := vQtde + 1;
    
  END LOOP;
  CLOSE c_Dados;
  GRAVA_MENSAGEM('Qtde=' || vQtde);
END;
/
SELECT MENSAGEM FROM TS_GRAVA_MENSAGEM ORDER BY TS_MENSAGEM;

                    
SELECT OWNER,OBJECT_NAME
FROM ALL_OBJECTS
WHERE OBJECT_NAME LIKE 'F%MES%';

SELECT *
FROM ALL_SYNONYMS
WHERE SYNONYM_NAME LIKE 'FS%';

	  AND COD_PERIODO   = v_Dados.COD_PERIODO
	  AND COD_PESSOA    = v_Dados.COD_PESSOA
	  AND COD_FIP_GF    = v_Dados.COD_FIP_GF
	  AND COD_GRUPO_FIN = v_Dados.COD_GRUPO_FIN
	  AND COD_SERVICO   = v_Dados.COD_SERVICO
	  AND COD_PARCELA   = v_Dados.COD_PARCELA

						AND DT_BASE     = v_Dados.DT_BASE
						AND NRO_SEQ_FAT = v_Dados.NRO_SEQ_FAT
                        
                        
                        

DROP TABLE TS_GRAVA_MENSAGEM;
CREATE TABLE TS_GRAVA_MENSAGEM (TS_MENSAGEM TIMESTAMP,MENSAGEM VARCHAR2(4000));

DECLARE
  vProcessou VARCHAR2(1);
  vQtde_Ok NUMBER;
  vQtde_Erro NUMBER;
  CURSOR c_Dados IS SELECT *
                    FROM TCB_CONTR_FILA_EVENTOS
                    WHERE STATUS = 'A'
                    AND TIPO_ACAO = 'BDT_FAT_GR';
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
SELECT MENSAGEM FROM TS_GRAVA_MENSAGEM ORDER BY TS_MENSAGEM;
select * from TCB_CONTR_FILA_EVENTOS ORDER BY ID_EVENTO DESC;

select tipo_acao, count(*) from TCB_CONTR_FILA_EVENTOS group BY tipo_acao;
SELECT *
FROM TCB_CONTR_FILA_EVENTOS
WHERE STATUS = 'E';

