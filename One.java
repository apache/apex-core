public class One
{
   public static void doitf(byte[] doit)
   {
        doit = new byte[100];
   }

   public static void main(String[] argv)
   {
      byte[] doit = new byte[10];
      doitf(doit);
      System.out.println("length = " + doit.length);
   }
}
