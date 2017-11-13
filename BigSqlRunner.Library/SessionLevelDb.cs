using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace BigSqlRunner.Library
{
    public class SessionLevelDb : IDisposable
    {
        public SessionSaveTypeEnum SessionSaveType { get; set; }
        public JsonLevelDb<string, string> SessionDb { get; set; }

        protected HashAlgorithm HashAlgorithm { get; set; } = MD5.Create();

        public SessionLevelDb(string sessionDbPath, SessionSaveTypeEnum sessionSaveType)
        {
            SessionDb = new JsonLevelDb<string, string>(sessionDbPath);
            SessionSaveType = sessionSaveType;

            var currentSaveType = SessionDb[nameof(SessionSaveType)];
            if (null == currentSaveType) SessionDb[nameof(SessionSaveType)] = SessionSaveType.ToString();
            else if (currentSaveType != SessionSaveType.ToString()) throw new ArgumentException($"value({sessionSaveType}) of specified {nameof(sessionSaveType)} mismatches with the one in session db: {sessionDbPath}", nameof(sessionSaveType));
        }

        public void Dispose()
        {
            if (null != SessionDb)
            {
                SessionDb.Dispose();
                SessionDb = null;
            }
        }

        protected string GetHash(string sqlUnit) => HelperFns.ComputeHash(sqlUnit?.Trim(), HashAlgorithm);

        public bool IsSqlUnitAlreadyExecuted(int sqlUnitIndex, string sqlUnit)
        {
            switch (SessionSaveType)
            {
                case SessionSaveTypeEnum.SqlUnitIndex:
                    return IsSqlUnitAlreadyExecuted(sqlUnitIndex);

                case SessionSaveTypeEnum.SqlUnitHash:
                    return IsSqlUnitAlreadyExecuted(sqlUnit);

                default: throw new ArgumentException($"unknown value of enum {nameof(SessionSaveTypeEnum)}: {SessionSaveType}", nameof(SessionSaveType));
            }
        }

        public bool IsSqlUnitAlreadyExecuted(int sqlUnitIndex)
        {
            if (SessionSaveTypeEnum.SqlUnitIndex != SessionSaveType) throw new ArgumentException($"{nameof(SessionSaveType)} of type {SessionSaveType} cannot be accessed by sql unit index", nameof(SessionSaveType));

            var alreadyExecuted = SessionDb[sqlUnitIndex.ToString()];
            if (true.ToString().ToLower() == alreadyExecuted?.ToLower()) return true;

            return false;
        }

        public bool IsSqlUnitAlreadyExecuted(string sqlUnit)
        {
            if (SessionSaveTypeEnum.SqlUnitHash != SessionSaveType) throw new ArgumentException($"{nameof(SessionSaveType)} of type {SessionSaveType} cannot be accessed by sql unit index", nameof(SessionSaveType));

            var sqlUnitHash = GetHash(sqlUnit);
            var alreadyExecuted = SessionDb[sqlUnitHash];
            if (true.ToString().ToLower() == alreadyExecuted?.ToLower()) return true;

            return false;
        }

        public void SetSqlUnitExecuteStatus(int sqlUnitIndex, string sqlUnit, bool alreadyExecuted)
        {
            switch (SessionSaveType)
            {
                case SessionSaveTypeEnum.SqlUnitIndex:
                    SetSqlUnitExecuteStatus(sqlUnitIndex, alreadyExecuted);
                    break;

                case SessionSaveTypeEnum.SqlUnitHash:
                    SetSqlUnitExecuteStatus(sqlUnit, alreadyExecuted);
                    break;

                default: throw new ArgumentException($"unknown value of enum {nameof(SessionSaveTypeEnum)}: {SessionSaveType}", nameof(SessionSaveType));
            }

            return;
        }

        public void SetSqlUnitExecuteStatus(int sqlUnitIndex, bool alreadyExecuted)
        {
            if (SessionSaveTypeEnum.SqlUnitIndex != SessionSaveType) throw new ArgumentException($"{nameof(SessionSaveType)} of type {SessionSaveType} cannot be accessed by sql unit index", nameof(SessionSaveType));

            SessionDb[sqlUnitIndex.ToString()] = alreadyExecuted.ToString().ToLower();
        }

        public void SetSqlUnitExecuteStatus(string sqlUnit, bool alreadyExecuted)
        {
            if (SessionSaveTypeEnum.SqlUnitHash != SessionSaveType) throw new ArgumentException($"{nameof(SessionSaveType)} of type {SessionSaveType} cannot be accessed by sql unit index", nameof(SessionSaveType));

            var sqlUnitHash = GetHash(sqlUnit);
            SessionDb[sqlUnitHash] = alreadyExecuted.ToString().ToLower();
        }
    }
}
