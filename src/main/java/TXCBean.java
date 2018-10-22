import java.io.Serializable;

public class TXCBean implements Serializable {
    private String ID,JSBH,HPHM,KKBH,CREATE_TIME;

    public TXCBean(String ID, String JSBH, String HPHM, String KKBH, String CREATE_TIME) {
        this.ID = ID;
        this.JSBH = JSBH;
        this.HPHM = HPHM;
        this.KKBH = KKBH;
        this.CREATE_TIME = CREATE_TIME;
    }

    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public String getJSBH() {
        return JSBH;
    }

    public void setJSBH(String JSBH) {
        this.JSBH = JSBH;
    }

    public String getHPHM() {
        return HPHM;
    }

    public void setHPHM(String HPHM) {
        this.HPHM = HPHM;
    }

    public String getKKBH() {
        return KKBH;
    }

    public void setKKBH(String KKBH) {
        this.KKBH = KKBH;
    }

    public String getCREATE_TIME() {
        return CREATE_TIME;
    }

    public void setCREATE_TIME(String CREATE_TIME) {
        this.CREATE_TIME = CREATE_TIME;
    }
}
