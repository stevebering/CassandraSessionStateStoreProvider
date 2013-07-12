using System.Web.Mvc;

namespace CassandraSessionStateSample.Controllers
{
    public class CassandraSessionStateController : Controller
    {
        private const string SessionStateKey = "__key";

        public ActionResult Index() {

            ViewBag.Value = Session[SessionStateKey];
            return View();
        }

        public ActionResult SetValue(string value) {

            Session[SessionStateKey] = value;
            return RedirectToAction("Index");
        }
    }
}
