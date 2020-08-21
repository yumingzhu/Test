package Dec.DecTest.bean;

public class Imp {
      private String id;
      private String pid;
      private Banner  banner;
      private Nativead nativead;
      private Video video;
      private int pos;
      private int bidtype;
      private Pmp pmp;
      private Ext ext;
}
class  Pmp{
     private Object[] deals;

    public Object[] getDeals() {
        return deals;
    }

    public void setDeals(Object[] deals) {
        this.deals = deals;
    }
}
class  Ext{
     private   int atype;

    public int getAtype() {
        return atype;
    }

    public void setAtype(int atype) {
        this.atype = atype;
    }
}

