import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.UUID;

public class RunSql {
	public static void main(String[] args) {
		String uid = UUID.randomUUID().toString();
		System.out.println("===========================================================");
		System.out.println("=====\t" + uid);
		System.out.println("=====\t" + new Timestamp((new java.util.Date()).getTime()));
		System.out.println("===========================================================");
		URL location = RunSql.class.getResource("RunSql.class");
		String locationPath = location.getPath().replace("RunSql.class", "");
		System.out.println("locationPath: " + locationPath);

		String flavor = null;
		String server = null;
		String database = null;
		String jarFile = null;
		String username = null;
		String password = null;

		String jar = null;
		String dbdriver = null;
		String url = null;
		String sql = null;
		String sqlFile = null;

		if (args.length >= 1) {
			flavor = args[0];
		}
		if (args.length >= 6) {
			if ("mysql".equalsIgnoreCase(flavor) || "sqlserver".equalsIgnoreCase(flavor)) {
				server = args[1];
				database = args[2];
				username = args[3];
				password = args[4];
				if ("-f".equalsIgnoreCase(args[5])) {
					sqlFile = args[6];
					try {
						System.out.println("sql file: " + locationPath + sqlFile);
						sql = readFile(locationPath + sqlFile);
					} catch (IOException e) {
						sql = null;
						e.printStackTrace();
					}
				} else {
					sql = args[5];
				}
			} else if ("manual".equalsIgnoreCase(flavor) && args.length >= 7) {
				jarFile = args[1];
				dbdriver = args[2];
				url = args[3];
				username = args[4];
				password = args[5];
				if ("-f".equalsIgnoreCase(args[6])) {
					sqlFile = args[7];
					try {
						sql = readFile(locationPath + sqlFile);
					} catch (IOException e) {
						sql = null;
						e.printStackTrace();
					}
				} else {
					sql = args[6];
				}
			}
		}

		if ("mysql".equalsIgnoreCase(flavor)) {
			// mysql example
			jar = "jar:file:" + locationPath + "mysql-connector-java-5.0.8-bin.jar!/";
			dbdriver = "com.mysql.jdbc.Driver";
			url = "jdbc:mysql://" + server + "/" + database
					+ "?useUnicode=true&characterEncoding=UTF-8&useFastDateParsing=false&connectTimeout=0&socketTimeout=0&autoReconnect=true";
		} else if ("sqlserver".equalsIgnoreCase(flavor)) {
			jar = "jar:file:" + locationPath + "sqljdbc4.jar!/";
			dbdriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
			url = "jdbc:sqlserver://" + server + ";databaseName=" + database + ";";
		} else if ("manual".equalsIgnoreCase(flavor)) {
			jar = "jar:file:" + locationPath + jarFile + "!/";
			dbdriver = "com.mysql.jdbc.Driver";
		} else if ("test".equalsIgnoreCase(flavor)) {
			jar = "jar:file:" + locationPath + "sqljdbc4.jar!/";
			dbdriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
			url = "jdbc:sqlserver://lwsqldev2.lifeway.org:1433;databaseName=r5liferaydb;user=r5liferay;password=R5l21248252;applicationName=MinistryGrid;";
			username = "r5liferay";
			password = "R5l21248252";
			sql = "select 1 as 'test'";
		} else if ("help".equalsIgnoreCase(flavor)) {
			System.out.println("arg[1]: flavor = mysql or sqlserver");
			System.out.println("\targ[2]: server");
			System.out.println("\targ[3]: database");
			System.out.println("\targ[4]: username");
			System.out.println("\targ[5]: password");
			System.out.println("\targ[6]: [sqlStatement] -or- [-f sqlFile]");
			System.out.println("arg[1]: flavor = manual");
			System.out.println("\targ[2]: jar file name");
			System.out.println("\targ[3]: database driver");
			System.out.println("\targ[4]: connection url");
			System.out.println("\targ[5]: username");
			System.out.println("\targ[6]: password");
			System.out.println("\targ[7]: [sqlStatement] -or- [-f sqlFile]");
		}

		if (jar != null && dbdriver != null && url != null && username != null && password != null && sql != null && jar.length() > 0 && dbdriver.length() > 0
				&& url.length() > 0 && username.length() > 0 && password.length() > 0 && sql.length() > 0) {

			System.out.println("jar: " + jar);
			System.out.println("dbdriver: " + dbdriver);
			System.out.println("url: " + url);
			System.out.println("username: " + username);
			System.out.println("password: " + password);
			//System.out.println("sql: " + sql);
			Statement stmt = null;
			Connection con;
			try {
				RunSql runSql = new RunSql();
				URL u = new URL(jar);
				URLClassLoader ucl = new URLClassLoader(new URL[] { u });
				Driver d = (Driver) Class.forName(dbdriver, true, ucl).newInstance();
				DriverManager.registerDriver(runSql.new DriverShim(d));
				con = DriverManager.getConnection(url, username, password);
				stmt = con.createStatement();

				//System.out.println("sql query " + pstmt.toString());
				if (stmt.execute(sql)) {
					ResultSet rs = stmt.getResultSet();
					ResultSetMetaData rsmd = rs.getMetaData();
					int numberOfColumns = rsmd.getColumnCount();
					int row = 1;
					for (int col = 1; col <= numberOfColumns; col++) {
						if (col == 1)
							System.out.print("count,\t");
						if (col > 1)
							System.out.print(",\t");
						String columnName = rsmd.getColumnName(col);
						System.out.print(columnName);
					}
					System.out.println("");
					while (rs.next()) {
						System.out.print(row + ",\t");
						for (int col = 1; col <= numberOfColumns; col++) {
							if (col > 1)
								System.out.print(",\t");
							System.out.print(rs.getString(col));
						}
						System.out.println("");
						row++;
					}
				} else {
					int count = stmt.getUpdateCount();
					System.out.println("query count: " + count);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} finally {
				if (stmt != null) {
					try {
						stmt.close();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		} else {
			System.out.println("flavor is required [mysql, sqlserver, manual]");
		}
		System.out.println("===========================================================");
		System.out.println("=====\t" + uid);
		System.out.println("=====\t" + new Timestamp((new java.util.Date()).getTime()));
		System.out.println("===========================================================");
		System.out.println("**********************************************************************************************************************");
		System.out.println("**********************************************************************************************************************");
	}
	
	public static String readFile(final String path) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(path));
        String line  = null;
        StringBuilder stringBuilder = new StringBuilder();
        String ls = System.getProperty("line.separator");
        while( ( line = reader.readLine() ) != null ) {
            stringBuilder.append( line );
            stringBuilder.append( ls );
        }
        reader.close();
        return stringBuilder.toString().trim();
     }

	class DriverShim implements Driver {
		private Driver driver;

		DriverShim(Driver d) {
			this.driver = d;
		}

		public boolean acceptsURL(String u) throws SQLException {
			return this.driver.acceptsURL(u);
		}

		public Connection connect(String u, Properties p) throws SQLException {
			return this.driver.connect(u, p);
		}

		public int getMajorVersion() {
			return this.driver.getMajorVersion();
		}

		public int getMinorVersion() {
			return this.driver.getMinorVersion();
		}

		public DriverPropertyInfo[] getPropertyInfo(String u, Properties p) throws SQLException {
			return this.driver.getPropertyInfo(u, p);
		}

		public boolean jdbcCompliant() {
			return this.driver.jdbcCompliant();
		}
	}
}