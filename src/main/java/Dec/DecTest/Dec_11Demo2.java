package Dec.DecTest;

public class Dec_11Demo2 {
    public static void main(String[] args) {
        {
//            for(int i=0;i<9;i++)
//            {
//                if(i!=5)
//                    continue;
//                System.out.println("i="+i);
//            }
            //-----------------
            for(int i=0;i<5;i++)
            {
                System.out.println("i="+i);
                for(int j=0;j<5;j++) {
                    if(j>=i)
                        break ;
                        System.out.print("j="+j);
                }
            }
        }

    }
}
