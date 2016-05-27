import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.StringTokenizer;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class bigdata_mlb 
{
    
    
    public static void main(String [] args ) throws IOException, SQLException, Exception
    {
       String allteam="http://www.msn.com/zh-tw/sports/mlb/team-stats";    //要抓的網址
       webcatch team = new webcatch(allteam,"allteam.txt");              
       //抓回html
       String location ="C:\\\\Users\\\\401637425.CSIE-103\\\\Documents\\\\NetBeansProjects\\\\JavaApplication2\\\\allteam.txt";//allteam檔案位置
       String location_1stclnup ="C:\\\\Users\\\\401637425.CSIE-103\\\\Documents\\\\NetBeansProjects\\\\JavaApplication2\\\\allteam_cleanup.txt";//allteam檔案位置
       
       FileWriter fw=new FileWriter("allteam_cleanup.txt"); 
       BufferedWriter bw=new BufferedWriter(fw);
        
       cleardata cl =new cleardata(location);                           
       //清理雜訊並寫入allteam_cleanup.txt
       getData pos = new getData(location_1stclnup,"team.txt","40201000000000226");
        file ff=new file("onlyinfo.txt");
        intoSQL ss=new intoSQL("onlyinfo.txt");
     
    }
}
class mssql 
{
     mssql(){}
     void insert(String sql_insert)throws Exception
     {
        String conUrl = "jdbc:sqlserver://localhost:1433;databaseName=rr114;user=sa;password=sa;";
        Connection con = null;
            try 
            {
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                con = DriverManager.getConnection(conUrl);
                String SQL = sql_insert;
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(SQL);
            }
            catch (Exception ex) 
            {
                ex.printStackTrace();
            }
            
     }
     void select(String sql_select)
     {
         String conUrl = "jdbc:sqlserver://localhost:1433;databaseName=rr114;user=sa;password=sa;";
        Connection con = null;
            try 
            {
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                con = DriverManager.getConnection(conUrl);
                String SQL = sql_select;
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(SQL);
            }
            catch (Exception ex) 
            {
                ex.printStackTrace();
            }
     }
}

class intoSQL
{
    intoSQL(String filename) throws FileNotFoundException, IOException, Exception
    {
         FileReader fr=new FileReader("onlyinfo.txt");
      BufferedReader br=new BufferedReader(fr);
      
      int ch;
      char c;
      String sum="" ;
      int table_count=0;
      int team_count=0;
      String team="",a="",ab="",hits="",hr="",rbi="",sb="",r="",bb="",k="",avg="",obp="",slg="";
      
      mssql sql=new mssql();
       while ((ch=br.read()) != -1) //讀ascii
       {
          
        c=(char)ch;//ascii變成字元
        
        if(c==32)//讀到空白佔存清空
        {
            
            
            
            table_count++;
            if(table_count==2)//a
            {
                a=sum;
                sum="";
            }
            else if(table_count==3)//ab
            {
                ab=sum;
                sum="";
            }
            else if(table_count==4)//hits
            {
                hits=sum;
                sum="";
            }
            else if(table_count==5)//hr
            {
                hr=sum;
                sum="";
            }
            else if(table_count==6)//rbi
            {
                rbi=sum;
                sum="";
            }
            else if(table_count==7)//sb
            {
                sb=sum;
                sum="";
            }
            else if(table_count==8)//r
            {
                r=sum;
                sum="";
            }
            else if(table_count==9)//bb
            {
                bb=sum;
                sum="";
            }
            else if(table_count==10)//k
            {
                k=sum;
                sum="";
            }
            else if(table_count==11)//avg
            {
                avg=sum;
                sum="";
            }
            else if(table_count==12)//obp
            {
                obp=sum;
                sum="";
            }
            else if(table_count==13)//slg
            {
                slg=sum;
                sum="";
            }
            
            if(table_count==13)
            {
                team_count++;
            if(team_count==1)
            {
             team="PIT";   
            }
            else if(team_count==2)
            {
                team="COL";
            }
            else if(team_count==3)
            {
                team="MIA";
            }
            else if(team_count==4)
            {
                team="STL";
            }
            if(team_count==4)
            {
                team_count=0;
            }
                table_count=1;
                sql.insert("Insert into game(team,a,ab,hits,hr,rbi,sb,r,bb,k,avg,obp,slg)values('"+team+"',"+a+","+ab+","+hits+","+hr+","+rbi+","+sb+","+r+","+bb+","+k+","+avg+","+obp+","+slg+")");
                System.out.println("insert : "+a+" "+ab+" "+hits+" "+hr+" "+rbi+" "+sb+" "+r+" "+bb+" "+k+" "+avg+" "+obp+" "+slg);
                a="";
                ab="";
                hits="";
                hr="";
                rbi="";
                sb="";
                r="";
                bb="";
                k="";
                avg="";
                obp="";
                slg="";
            }          
            sum="";   
        }
        if(c!=32)
        {
            sum=sum+c;    
        } 
    }
        
}}
class file
{
    file(String filename) throws FileNotFoundException, IOException
    {
        try
        {
      FileWriter fw=new FileWriter(filename); 
      BufferedWriter bw=new BufferedWriter(fw);
      FileReader fr=new FileReader("team.txt");
      BufferedReader br=new BufferedReader(fr);
      mssql sql=new mssql();
      int ch;
      char c;
      String sum="" ;
      int count=0;
      int scount=0;
      boolean pt=false;
       while ((ch=br.read()) != -1) 
       {
           c=(char)ch;    
           if(c==95)//當讀到______
           {
              count++;
              c=32;
           }
           if(c==95)//當讀到______
           {
              count++;
              c=32;
           }
           if(count==13)
           {
              count=0;
              bw.write("");
           }
           if(count>0)
              {
              //System.out.print(c);
               bw.write(c);               
              }
       }
        while ((ch=br.read()) != -1) 
       {
           c=(char)ch;    
           if(c==95)//當讀到______
           {
              count++;
              c=32;
           }
           if(c==95)//當讀到______
           {
              count++;
              c=32;
           }
           if(count==13)
           {
              count=0;
              System.out.println("");
              bw.write("\n\n");
           }
           if(count>0)
              {
              System.out.print(c);
               bw.write(c);
              }            
       }
          bw.flush();
          fw.close();
          bw.close();
    }
    catch (IOException e) {System.out.println(e);}
    }
    }


class getData
{
    getData(String location,String filename,String code)throws IOException 
    {
        FileWriter fw=new FileWriter(filename); 
        BufferedWriter bw=new BufferedWriter(fw);
        try //清雜訊+整理html對齊
        {
                File file = new File(location);
                BufferedReader br = new BufferedReader(new FileReader(file));
                String t;
                while( ( t = br.readLine()) != null )//讀取每一行html
                {
                  if(t.startsWith("4"))  
                  { 
                       //System.out.println(t);
                       bw.write(t);
                  }
                  else
                  {
                      System.out.print("");
                  }
                }
                 bw.flush();
                 fw.close();
                 bw.close();//System.out.println("抓取隊伍成功 !");
        }
        catch(Exception ex)
        {
                ex.printStackTrace();
        }   
    }
}
class cleardata
{
    cleardata(String location) throws IOException 
    {
        FileWriter fw=new FileWriter("allteam_cleanup.txt"); 
        BufferedWriter bw=new BufferedWriter(fw);
        try //清雜訊+整理html對齊
        {
                File file = new File(location);
                BufferedReader br = new BufferedReader(new FileReader(file));
                String t;
                while( ( t = br.readLine()) != null )//讀取每一行html
                {
                       t=t.replaceAll("<ahref", "");
                       t=t.replaceAll("</div>", "");
                       t=t.replaceAll("<td>", "");
                       t=t.replaceAll("<table class=\"tableview statstable clearfix\" tabindex=\"0\"><thead><tr><th class=\"hide1\">", "");
                       t=t.replaceAll("</th><th class=\"teamname\" colspan=\"2\"><a class=\"\" href=\"/zh-tw/sports/mlb/team-stats/sp-s-team\">隊伍</a></th><th class=\"teamtla\"", "");
                       t=t.replaceAll("colspan=\"2\"><a class=\"\" href=\"/zh-tw/sports/mlb/team-stats/sp-s-team\">隊伍</a></th><th title=\"出賽場次\">", "");
                       t=t.replaceAll(" <a class=\"\" href=\"/zh-tw/sports/mlb/team-stats/sp-s-gp\">出賽</a></th><th class=\"hide1\" title=\"打者完成有效打擊的次數\"><a class=\"\" href=\"/zh-tw/sports/mlb/team-stats/sp-s-ab\">", "");
                       t=t.replaceAll("打數</a></th><th class=\"hide1\" title=\"安打\"><a class=\"\" href=\"/zh-tw/sports/mlb/team-stats/sp-s-h\">安打</a></th><th title=\"全壘打數\"><a class=\"\" href=\"/zh-tw/sports/mlb/team-stats/sp-s-hr\">", "");
                       t=t.replaceAll("全壘打</a></th><th title=\"打點\"><a class=\"\" href=\"/zh-tw/sports/mlb/team-stats/sp-s-rbi\">打點</a></th><th class=\"hide123\" title=\"盜壘數\"><a class=\"\" href=\"/zh-tw/sports/mlb/team-stats/sp-s-sb\">盜壘</a></th><th class=\"hide1\" title=\"得分\"><a class=\"\" href=\"/zh-tw/sports/mlb/team-stats/sp-s-r\">得分</a></th><th class=\"hide123\" title=\"保送\"><a class=\"\" href=\"/zh-", "");
                       t=t.replaceAll("tw/sports/mlb/team-stats/sp-s-bb\">保送</a></th><th class=\"hide1\" title=\"三振數\"><a class=\"\" href=\"/zh-tw/sports/mlb/team-stats/sp-s-so\">三振</a></th><th title=\"打擊率\"><a class=\"desc\" href=\"/zh-tw/sports/mlb/team-stats/sp-s-avg-a-true\">打擊率</a></th><th class=\"hide1\" title=\"上壘率\"><a class=\"\" href=\"/zh-tw/sports/mlb/team-stats/sp-s-obp\">上壘率</a></th><th class=\"hide1\" title=\"每次打擊平均攻佔的壘包數\"><a class=\"\" href=\"/zh-tw/sports/mlb/team-stats/sp-s-slg\">長打率</a></th></tr></thead><tbody><tr class=\"rowlink\" data-link=\"/zh-tw/sports/mlb/", "");
                       t=t.replaceAll("<img alt=\"", ""); 
                       t=t.replaceAll("<tr class=\"rowlink\" data-link=\"/zh-tw/sports/mlb/", "");
                       t=t.replaceAll(" tabindex=\"0\"><td class=\"hide1 ", "");
                       t=t.replaceAll("\"rankvalue\">3</td><td class=\"teamlogo\">", " ");
                       t=t.replaceAll("</a>", "");
                       t=t.replaceAll("</td>", "__");
                       t=t.replaceAll("<td class=\"hide1\">", "");
                       t=t.replaceAll("<td class=\"hide123\">", "");
                       t=t.replaceAll("<td class=\"hide123\">", "");
                       t=t.replaceAll("rankvalue\">", "");
                       t=t.replaceAll("<td class=\"teamname\" colspan=\"1\">", "");
                       t=t.replaceAll("rankvalue\">", "");
                       t=t.replaceAll("<td class=\"teamlogo\">", "");
                       t=t.replaceAll("/team/sp-id-", "");
                       t=t.replaceAll("</tbody></table> <div class=\"legend clearfix\"><span class=\" legenditem\"><span>出賽</span> - 出賽場次</span><span class=\"hide1 legenditem\"><span>打數</span> - 打者完成有效打擊的次數</span><span class=\"hide1 legenditem\"><span>安打</span> - 安打</span><span class=\" legenditem\"><span>全壘打</span> - 全壘打數</span><span class=\" legenditem\"><span>打點</span> - 打點</span><span class=\"hide123 legenditem\"><span>盜壘</span> - 盜壘數</span><span class=\"hide1 legenditem\"><span>得分</span> - 得分</span><span class=\"hide123 legenditem\"><span>保送</span> - 保送</span><span class=\"hide1 legenditem\"><span>三振</span> - 三振數</span><span class=\" legenditem\"><span>打擊率</span> - 打擊率</span><span class=\"hide1 legenditem\"><span>上壘率</span> - 上壘率</span><span class=\"hide1 legenditem\"><span>長打率</span> - 每次打擊平均攻佔的壘包數</span> ", "");
                       t=t.replaceAll("&amp;h=30&amp;w=30&amp;m=6&amp;q=60&amp;o=t&amp;l=f\" width=\"30\" />__<a href=\"/zh-tw/sports/mlb/","\n");
                       t=t.replaceAll("<td class=\"teamtla\" colspan=\"1\"><a href=\"/zh-tw/sports/mlb/","\n");
                       t=t.replaceAll("</tr>","\n");
                       t=t.replaceAll("\\?f=PNG","");
                       t=t.replaceAll("height=\"30\" src=\"//img-s-msn-com.akamaized.net/tenant/amp/entityid/","");
                        t=t.replaceAll("科羅拉多落磯","");
                        //t=t.replaceAll("波士頓紅襪","");
                        t=t.replaceAll("匹茲堡海盜","");
                   t=t.replaceAll("邁阿密馬林魚","");
                   t=t.replaceAll("巴爾的摩金鶯","");
                   t=t.replaceAll("科羅拉多落磯","");
                   t=t.replaceAll("聖路易紅雀","");
                   t=t.replaceAll(">"," ");
                   t=t.replaceAll("__","_");
                   t=t.replaceAll("\" _"," ");    
                   t=t.replaceAll("_\" "," ");
                   t=t.replaceAll(",","");
                   if(t.startsWith("波"))
                       {
                            //System.out.println(t);
                            bw.write(t);
                       }
                }
                 bw.flush();
                 fw.close();
                 bw.close();
                System.out.println("第一階段清理雜訊成功 !");               
        }
        catch(Exception ex)
        {
                ex.printStackTrace();
        }
    }
}
class webcatch
{
     webcatch(String website,String filename) throws IOException//throws IOException?
    {
       {
           FileWriter fw=new FileWriter(filename); 
           BufferedWriter bw=new BufferedWriter(fw);
           HttpClient httpclient =new DefaultHttpClient(); 
           HttpGet httpget =new HttpGet(website);
           HttpResponse response =httpclient.execute(httpget);
           String responseString =EntityUtils.toString(response.getEntity());
            if(response.getStatusLine().getStatusCode() == HttpStatus.SC_OK)
            {
                         //System.out.println(responseString);
                         bw.write(responseString);
             }
             else
             {
                System.out.println("error");
                System.out.println(response.getStatusLine());
             }
          System.out.println(new Date());
          System.out.println("catch data success"+"\n"+"-------------------------------");
          System.out.println();
          httpget.releaseConnection();
          bw.flush();
          fw.close();
          bw.close();
       }
    }
}